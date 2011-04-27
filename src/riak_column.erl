%% -------------------------------------------------------------------
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%  http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_column, [Storage,Bucket,ColumnName]).
-author("Kresten Krab Thorup <krab@trifork.com>").

%%
%% Based on idea by Erik Soe Soerensen described here
%%
%%   http://polymorphictypist.blogspot.com/
%%          2011/04/multi-version-collections-in-riak.html
%%

-export([lookup/1,add/2,put/2,put/3,delete/1,fold/2]).

-record(main_group, { entries=[]    :: [entry()],
                      grouppointers=[] :: [bitstring()] }).
-record(group, {entries=[] :: [entry()] }).

-type vclock() :: vclock:vclock().
-type riak_object() :: riak_object:riak_object().

-type value() :: {vclock(),list()}.
-type entry() :: {binary(), value()}.


-define(GROUP_TOMBSTONE, <<"deleted_group">>).
-define(VALUE_TOMBSTONE, <<"deleted_value">>).

-ifdef(TEST).
-define(MAX_ENTRIES_PER_GROUP, 3).
-define(edbg(M,A), error_logger:info_msg(M,A)).
-else.
-define(MAX_ENTRIES_PER_GROUP, 100).
-define(edbg(M,A), ok).
-endif.

%% @private
group_name(GroupP) when bit_size(GroupP) =< 160 ->
    BitSize = bit_size(GroupP),
    Bits = BitSize rem 8,
    <<ColumnName/binary,$#,BitSize,GroupP/bitstring,0:(8-Bits)/unsigned-unit:1>>.


-spec lookup(RowKey::binary()) -> {ok, value()} | {error, notfound}.
lookup(RowKey) when is_binary(RowKey) ->
    case get_main_group() of
        {ok, #main_group{entries=Elems,grouppointers=GroupPointers}, _} ->
            case lists:keyfind(RowKey, 1, Elems) of
                false ->
                    case listfind( bit_prefix_match(RowKey), GroupPointers ) of
                        {ok, GroupP} ->
                            ?edbg("looking up ~p (hash=~w)~nin ~p", [RowKey, crypto:sha(RowKey), group_name(GroupP)]),
                            case lookup_in_group(GroupP,RowKey) of
                                {ok,_}=V -> V;
                                {error,_}=E -> E
                            end;
                        notfound ->
                            {error, notfound}
                    end;
                {RowKey,_}=KVE ->
                    value_result(KVE)
            end
    end.

-spec fold(fun(({binary(), {vclock(), list()}}, term()) -> term), term()) -> term().
fold(Fun,Acc0) ->
    case get_main_group() of
        {ok, #main_group{entries=[],grouppointers=GroupPs}, _} ->
            lists:foldl(fun(GroupP,Acc) ->
                                case get_group(GroupP) of
                                    {ok, #group{ entries=Entries }, _} ->
                                        lists:foldl(Fun, Acc, Entries);
                                    {error, notfound} ->
                                        Acc
                                end
                        end,
                        Acc0,
                        GroupPs);
        {ok, #main_group{entries=Entries,grouppointers=[]}, _} ->
            lists:foldl(Fun, Acc0, Entries);
        {error, notfound} ->
            Acc0
    end.



add(RowKey, Value) when is_binary(RowKey) ->
    case lookup(RowKey) of
        {error, notfound} ->
            THIS:put(RowKey, {vclock:fresh(), [Value]});
        {ok, {VC, OldValues}} ->
            update(RowKey, {vclock:increment(Storage:get_client_id(), VC), [Value| [V || V<-OldValues, V =/=Value]]})
    end.

put(RowKey, {VC, [Value]}) when is_binary(RowKey) ->
    put(RowKey, {VC, [Value]}, []).

put(RowKey, {VC, [Value]}, Options) ->
    {ok, Stored} = update(RowKey, {vclock:increment(Storage:get_client_id(), VC), [Value]}),
    case proplists:get_bool(returnvalue, Options) of
        true -> {ok, Stored};
        false -> ok
    end.

-spec update(RowKey::binary(), Value::value()) -> {ok, value()}.
update(RowKey, {VC,Values}) when is_list(Values) ->
    case get_main_group() of
        {ok, #main_group{entries=Elems,grouppointers=[]}, RObj} ->
            case lists:keyfind(RowKey, 1, Elems) of
                false ->
                    Merged = {VC,Values},
                    NewEntries = lists:keysort(1,[ {RowKey, Merged} | Elems ]);
                {RowKey,ELEM} ->
                    Merged = merge_values({VC,Values}, ELEM),
                    NewEntries = lists:keyreplace(RowKey,1,Elems,{RowKey, Merged})
            end,
            if
                length(NewEntries) > ?MAX_ENTRIES_PER_GROUP ->
                    %% split root
                    {Group0,Group1} = split_by_prefix(0, NewEntries),
                    ok = update_group(riak_object:new(Bucket, group_name(<<0:1>>), Group0)),
                    ok = update_group(riak_object:new(Bucket, group_name(<<1:1>>), Group1)),
                    ok = update_main_group(RObj, #main_group{ grouppointers=[<<0:1>>, <<1:1>>] });
                true ->
                    %% store root
                    ok = update_main_group(RObj, #main_group{ entries=NewEntries })
            end,
            {ok, Merged};
        {ok, #main_group{entries=[],grouppointers=Groups}=TheMainGroup, RObj} ->
            {ok, GroupP} = listfind( bit_prefix_match(RowKey), Groups),
            ?edbg("storing ~p into ~p", [RowKey, group_name(GroupP)]),
            case update_group(GroupP, RowKey, {VC,Values}) of
                {ok, [], Merged} ->
                    %% must re-update main group to force later read repair if different
                    update_main_group(RObj, TheMainGroup),
                    {ok, Merged};
                {ok, [GP1,GP2]=SplitGroupPs, Merged} when is_bitstring(GP1), is_bitstring(GP2) ->
                    NewMainGroup = #main_group{ grouppointers= lists:sort( SplitGroupPs ++ [R || R <- Groups, R =/= GroupP] ) },
                    ok = update_main_group(RObj, NewMainGroup),
                    {ok, Merged}
            end
    end.


update_group(RObj) ->
    MD = dict:store(<<"Links">>, [{{Bucket, ColumnName}, "column"}],
                    riak_object:get_update_metadata(RObj)),
    Storage:put(riak_object:update_metadata(RObj, MD)).


update_main_group_links(RObj) ->
    #main_group{grouppointers=Pointers} = riak_object:get_update_value(RObj),
    GroupLinks = lists:foldl(fun(GroupP,Links) ->
                                     [{{Bucket, group_name(GroupP)}, "colum_group"} | Links]
                             end,
                             [],
                             Pointers),
    MD = dict:store(<<"Links">>, GroupLinks, riak_object:get_update_metadata(RObj)),
    RObj2 = riak_object:update_metadata(RObj, MD),
    RObj2.

update_main_group(RObj,#main_group{}=MainGroup) ->
    RObj1 = riak_object:update_value(RObj, MainGroup),
    RObj2 = update_main_group_links( RObj1 ),
    Storage:put(RObj2).

split_by_prefix(N,List) when N<160 ->
    split_by_prefix(N, List, {[], []}).

split_by_prefix(_, [], {L0,L1}) ->
    {#group{entries=L0},#group{entries=L1}};
split_by_prefix(Bits, [{RowKey,Value}|Rest], {L0,L1}) ->
    case crypto:sha(RowKey) of
        <<_:Bits/bitstring,0:1,_/bitstring>> ->
            split_by_prefix(Bits, Rest, {[{RowKey,Value}|L0], L1});
        <<_:Bits/bitstring,1:1,_/bitstring>> ->
            split_by_prefix(Bits, Rest, {L0, [{RowKey,Value}|L1]})
    end.


update_group(GroupP, RowKey, Value) ->
    case get_group(GroupP) of
        {ok, #group{ entries=Elems }, RObj} ->
            case lists:keyfind(RowKey, 1, Elems) of
                {RowKey,OrigValue} ->
                    Merged = merge_values(Value, OrigValue),
                    NewEntries = lists:keyreplace(RowKey,1,Elems,{RowKey,Merged}),
                    update_group(riak_object:update_value(RObj, #group{entries=NewEntries})),
                    {ok, [], Merged};

                false ->
                    Merged = Value,
                    NewEntries = lists:keysort(1,[ {RowKey, Merged} | Elems ]),

                    %% group needs splitting?
                    if length(NewEntries) > ?MAX_ENTRIES_PER_GROUP ->
                            {Group0,Group1} = split_by_prefix(bit_size(GroupP), NewEntries),
                            Bits = bit_size(GroupP),
                            GroupP0 = <<GroupP:Bits/bitstring, 0:1>>,
                            GroupP1 = <<GroupP:Bits/bitstring, 1:1>>,
                            ok = update_group(riak_object:new(Bucket, group_name(GroupP0), Group0)),
                            ok = update_group(riak_object:new(Bucket, group_name(GroupP1), Group1)),
                            ok = update_group(riak_object:update_value(RObj, ?GROUP_TOMBSTONE)),
                            {ok, [GroupP0,GroupP1], Merged};

                        true ->
                            update_group(riak_object:update_value(RObj, #group{entries=NewEntries})),
                            {ok, [], Merged}
                    end

            end;
        {error, notfound} ->
            ok = update_group(riak_object:new(Bucket, group_name(GroupP), #group{entries=[{RowKey, [Value]}]})),
            {ok, [], Value}
    end.


-spec value_result(entry()) -> {ok, value()} | {error, notfound}.

value_result({_,VE}) ->
    case VE of
        {_, [?VALUE_TOMBSTONE]} ->
            {error, notfound};
        {VC,Values} ->
            {ok, {VC, [V || V <- Values, V =/= ?VALUE_TOMBSTONE]}}
    end.


delete(RowKey) ->
    case lookup(RowKey) of
        {ok, {VC, _}} ->
            THIS:put(RowKey, {VC, [?VALUE_TOMBSTONE]});
        {error, notfound} ->
            ok
    end.


-spec bit_prefix_match(RowKey::binary()) -> fun( (GroupP::bitstring()) -> boolean() ).
bit_prefix_match(RowKey) ->
    KeyHash = crypto:sha(RowKey),
    fun(GroupP) ->
            Bits = bit_size(GroupP),
            PadBits = 160-Bits,
            case KeyHash of
                <<GroupP:Bits/bitstring-unit:1, _:(PadBits)/bitstring-unit:1>> ->
                    true;
                _ ->
                    false
            end
    end.

-spec listfind(fun((T) -> boolean()), list(T)) -> notfound | {ok, T}.
listfind(_Fun, []) ->
    notfound;
listfind(Fun, [H|T]) ->
    case Fun(H) of
        true -> {ok, H};
        false -> listfind(Fun, T)
    end.

keyzip(N,Fun,TupList1,TupList2) ->
    keyzip(N,Fun,TupList1,TupList2,[]).

%keyzip(N,TupList1,TupList2) ->
%    keyzip(N,fun(E1,E2) -> {E1,E2} end,TupList1,TupList2,[]).

keyzip(N,Fun,[Tup1|Rest1]=L1,[Tup2|Rest2]=L2, Result) ->
    Key1 = element(N, Tup1),
    Key2 = element(N, Tup2),
    if
        Key1 =:= Key2 ->
            keyzip(N,Fun,Rest1,Rest2,[Fun(Tup1,Tup2)|Result]);
        Key1 < Key2 ->
            keyzip(N,Fun,Rest1,L2,[Fun(Tup1,undefined)|Result]);
        true ->
            keyzip(N,Fun,L1,Rest2,[Fun(undefined,Tup2)|Result])
    end.

-spec get_main_group() -> {ok, #main_group{}, riak_object()} | {error, _}.
get_main_group() ->
    case Storage:get(Bucket, ColumnName) of
        {error, notfound} ->
            Storage:put(riak_object:new(Bucket, ColumnName, #main_group{}),
                        1, 0, 1000, [{returnbody, true}]),
            get_main_group();
        {error, E} ->
            {error, E};
        {ok, MainGroupObject} ->
            case riak_object:get_values(MainGroupObject) of
                [] ->
                    {error, notfound};
                [MainGroup] ->
                    {ok, MainGroup, MainGroupObject};
                MainGroups ->
                    %% do read repair
                    MergedMainGroup = lists:foldl(fun merge_main_groups/2, #main_group{}, MainGroups),
                    RObj = Storage:put(update_main_group_links(riak_object:update_value(MainGroupObject,
                                                                                        MergedMainGroup)),
                                       1, 0, 1000, [{returnbody, true}]),
                    {ok, MergedMainGroup, RObj}
            end
    end.

merge_main_groups(#main_group{entries=Elms1, grouppointers=Groups1},
                #main_group{entries=Elms2, grouppointers=Groups2}) ->
    #main_group{ entries=merge_entries(Elms1, Elms2),
                 grouppointers=merge_grouppointers(Groups1, Groups2) }.

merge_grouppointers(Groups1,Groups2) ->
    R = lists:umerge(Groups1,Groups2),
    {Dead,Alive} = compute_dead_or_live(R, {[],[]}),
    if Dead =:= [] -> ok;
       true -> proc_lib:spawn(fun() -> read_repair_dead_groups(Dead) end)
    end,
    Alive.

compute_dead_or_live([R1,R2|Rest], {Dead,Alive}) when bit_size(R1) < bit_size(R2) ->
    BS1 = bit_size(R1),
    case R2 of
        <<R1:BS1/bitstring-unit:1,_/bitstring>> ->
            compute_dead_or_live([R2|Rest], {[R1|Dead], Alive});
        _ ->
            compute_dead_or_live([R2|Rest], {Dead, [R1|Alive]})
    end;
compute_dead_or_live(Live, {Dead,Alive}) ->
    {Dead, lists:reverse(Alive, Live)}.

read_repair_dead_groups([]) ->
    ok;
read_repair_dead_groups([GroupP|Rest]) ->
    case get_group(GroupP) of
        {ok, #group{entries=Elms}, RObj} ->
            ok = bulk_update(Elms,binary),
            ok = Storage:put(riak_object:update_value(RObj,?GROUP_TOMBSTONE), 0);
        {error, E} ->
            error_logger:info_msg("read repair failed; ignoring error: ~p", [E]),
            ok
    end,
    read_repair_dead_groups(Rest).

%% TODO: make this work sensibly
bulk_update([], _) -> ok;
bulk_update([{K,BVs}|Elms], binary) ->
    update(K,BVs),
    bulk_update(Elms, binary);
bulk_update([{K,VEs}|Elms], term) ->
    update(K,VEs),
    bulk_update(Elms, term).

-spec merge_entries([entry()], [entry()]) -> [entry()].
merge_entries(Elms1,[]) ->
    Elms1;
merge_entries([],Elms2) ->
    Elms2;
merge_entries(Elms1,Elms2) ->
    keyzip(1,
           fun(Elm1,undefined) ->
                   Elm1;
              (undefined,Elm2) ->
                      Elm2;
              (Elm1,Elm2) ->
                   merge_entrie_pair(Elm1,Elm2)
           end,
           Elms1,
           Elms2).

merge_entrie_pair({Key,VE1}, {Key,VE2}) ->
    {Key, merge_values(VE1,VE2)}.

merge_values({VC1,ValueList1}=Elm1, {VC2,ValueList2}=Elm2) ->
    case vclock:descends(VC1,VC2) of
        true ->
            Elm1;
        false ->
            case vclock:descends(VC2,VC1) of
                true ->
                    Elm2;
                false ->
                    ValList = lists:umerge(lists:usort(ValueList1),lists:usort(ValueList2)),
                    {vclock:merge(VC1,VC2),ValList}
            end
    end.


-spec lookup_in_group(bitstring(),binary()) -> {ok, value()} | {error, notfound}.
lookup_in_group(Groupp, RowKey) ->
    case get_group(Groupp) of
        {ok, #group{entries=Elems}, _} ->
            case lists:keyfind(RowKey, 1, Elems) of
                false ->
                    {error, notfound};
                KVE ->
                    value_result(KVE)
            end;
        {error, _}=Error ->
            Error
    end.

-spec get_group(GroupP::bitstring()) -> {ok, #group{}, riak_object()} | {error, _}.
get_group(GroupP) ->
    case Storage:get(Bucket, group_name(GroupP)) of
        {error, E} ->
            {error, E};
        {ok, GroupObject} ->
            case [ GroupData ||
                     GroupData <- riak_object:get_values(GroupObject),
                     GroupData =/= ?GROUP_TOMBSTONE ] of

                %% TODO: read-repair if sibling is deleted

                [] ->
                    {error, notfound};

                [#group{}=Group] ->
                    {ok, Group, GroupObject};

                ManyGroups ->
                    error_logger:info_msg("ManyGroups: ~p", [ManyGroups]),
                    NewGroup = lists:foldl(fun(#group{entries=E1},#group{entries=E2}) ->
                                                   #group{entries=merge_entries(E1,E2)}
                                           end,
                                           #group{},
                                           ManyGroups),

                    %% read repair the group
                    {ok, RObj} =
                        Storage:put(riak_object:update_value(GroupObject, NewGroup),
                                    1, 0, 10000, [{returnbody, true}]),

                    {ok, NewGroup, RObj}
            end
    end.
