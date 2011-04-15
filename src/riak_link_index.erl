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

-module(riak_link_index).
-author("Kresten Krab Thorup <krab@trifork.com>").

-export([precommit/1,postcommit/1]).

-define(MD_LINKS,<<"Links">>).
-define(MD_DELETED,<<"X-Riak-Deleted">>).
-define(IDX_PREFIX,"idx@").
-define(JSPOOL_HOOK, riak_kv_js_hook).

-ifdef(DEBUG).
-define(debug(A,B),error_logger:info_msg(A,B)).
-else.
-define(debug(A,B),ok).
-endif.


precommit(Object) ->

    ?debug("precommit in ~p", [Object]),

    Bucket = riak_object:bucket(Object),
    Key = riak_object:key(Object),

    %% Indexing works in two phases: precommit will use a hook to add links as
    %%
    %%    </riak/IBucket/IKey>; riaktag="idx@Tag"
    %%
    %% to the object being stored.  Then postcommit creates empty-contents
    %% objects named IBucket/IKey, with links to this object thus:
    %%
    %%    <riak/Bucket/Key>; riaktag="Tag"
    %%

    case is_updated(Object) of
        true ->
            OldLinksToMe = get_index_links(riak_object:get_metadatas(Object)),
            [{MD,_Value}] = index_contents(Bucket,
                                           [{ riak_object:get_update_metadata(Object),
                                              riak_object:get_update_value(Object) }]),
            IndexedObject = riak_object:update_metadata(Object, MD);

        false ->
            {ok, StorageMod} = riak:local_client(),
            case StorageMod:get(Bucket, Key) of
                {ok, OldRO} ->
                    OldLinksToMe = get_index_links(riak_object:get_metadatas(OldRO));
                _ ->
                    OldLinksToMe = []
            end,
            MDVs = index_contents(Bucket,
                                  riak_object:get_contents(Object)),
            IndexedObject = riak_object:set_contents(Object, MDVs)
    end,

    %% compute links to add/remove in postcommit
    NewLinksToMe  = get_index_links(IndexedObject),
    LinksToRemove = ordsets:subtract(OldLinksToMe, NewLinksToMe),
    LinksToAdd    = ordsets:subtract(NewLinksToMe, OldLinksToMe),

    %% this only works in recent riak_kv master branch
    put(?MODULE, {LinksToAdd, LinksToRemove}),

    ?debug("precommit out ~p", [IndexedObject]),

    IndexedObject.

postcommit(Object) ->

    case erlang:erase(?MODULE) of
        {[], []} ->
            ok;
        {LinksToAdd, LinksToRemove} ->
            {ok, StorageMod} = riak:local_client(),
            Bucket = riak_object:bucket(Object),
            Key = riak_object:key(Object),
            add_links(StorageMod, LinksToAdd, Bucket, Key),
            remove_links(StorageMod, LinksToRemove, Bucket, Key),
            ok;
        _ ->
            error_logger:error_msg("error in pre/postcommit interaction", []),
            ok
    end.

add_links(StorageMod, Links, Bucket, Key) ->
    lists:foreach(fun({{IndexB,IndexK}, <<?IDX_PREFIX,Tag/binary>>}) ->
                          add_link(StorageMod, IndexB, IndexK, {{Bucket,Key},Tag})
                  end,
                  Links).

add_link(StorageMod, Bucket, Key, Link) ->
    update(
      fun(Object) ->
              Links = get_all_links(Object),
              IO1 = riak_object:update_value(Object, <<>>),
              riak_object:update_metadata
                (IO1, dict:store(?MD_LINKS,
                                 [Link] ++ Links,
                                 riak_object:get_update_metadata(IO1)))
      end,
      StorageMod, Bucket, Key).

remove_links(StorageMod, Links, Bucket, Key) ->
    lists:foreach(fun({{IndexB,IndexK}, <<?IDX_PREFIX,Tag/binary>>}) ->
                          remove_link(StorageMod, IndexB, IndexK, {{Bucket,Key},Tag})
                  end,
                  Links).

remove_link(StorageMod, Bucket, Key, Link) ->
    update(
      fun(Object) ->
              Links = get_all_links(Object),
              UpdLinks = ordsets:del_element(Link,Links),
              IO1 = riak_object:update_value(Object, <<>>),
              riak_object:update_metadata
                (IO1, dict:store(?MD_LINKS,
                                 UpdLinks,
                                 riak_object:get_update_metadata(IO1)))
      end,
      StorageMod, Bucket, Key).

update(Fun,StorageMod,Bucket,Key) ->
    Updated =
        case StorageMod:get(Bucket,Key) of
            {ok, Object} ->
                Fun(Object);
            _ ->
                Fun(riak_object:new(Bucket, Key, <<>>))
        end,

    StorageMod:put(Updated, 0).


get_index_links(MDList) ->
    ordsets:filter(fun({_, <<?IDX_PREFIX,_/binary>>}) ->
                           true;
                      (_) ->
                           false
                   end,
                   get_all_links(MDList)).

get_all_links(Object) when element(1,Object) =:= r_object ->
    get_all_links
      (case is_updated(Object) of
           true ->
               [riak_object:get_update_metadata(Object)]
                   ++ riak_object:get_metadatas(Object);
           false ->
               riak_object:get_metadatas(Object)
       end);

get_all_links(MetaDatas) when is_list(MetaDatas) ->
    Links = lists:foldl(fun(MetaData, Acc) ->
                                case dict:find(?MD_LINKS, MetaData) of
                                    error ->
                                        Acc;
                                    {ok, LinksList} ->
                                        LinksList ++ Acc
                                end
                        end,
                        [],
                        MetaDatas),

    ordsets:from_list(Links).

index_contents(Bucket, Contents) ->

    %% grab indexes from bucket properties
    {ok, IndexHooks} = get_index_hooks(Bucket),

    lists:map
      (fun({MD,Value}) ->
               case dict:find(?MD_DELETED, MD) of
                   {ok, "true"} ->
                       {remove_idx_links(MD),Value};
                   _ ->
                       NewMD = compute_indexed_md(MD, Value, IndexHooks),
                       {NewMD, Value}
               end
       end,
       Contents).

remove_idx_links(MD) ->
    %% remove any "idx#..." links
    case dict:find(?MD_LINKS, MD) of
        error ->
            MD;
        {ok, Links} ->
            dict:store
              (?MD_LINKS,
               lists:filter(fun({_,<<?IDX_PREFIX,_/binary>>}) ->
                                    false;
                               (_) ->
                                    true
                                  end,
                            Links),
               MD)
    end.


compute_indexed_md(MD, Value, IndexHooks) ->
    lists:foldl
      (fun({struct, PropList}=IndexHook, MDAcc) ->
               {<<"tag">>, Tag} = proplists:lookup(<<"tag">>, PropList),
               Links = case dict:find(?MD_LINKS, MDAcc) of
                           error -> [];
                           {ok, MDLinks} -> MDLinks
                       end,
               IdxTag = <<?IDX_PREFIX,Tag/binary>>,
               KeepLinks =
                   lists:filter(fun({{_,_}, TagValue}) -> TagValue =/= IdxTag end,
                                Links),
               NewLinksSansTag =
                   try apply_index_hook(IndexHook, MD, Value) of
                       {erlang, _, {ok, IL}} when is_list(IL) ->
                           IL;
                       {js, _, {ok, IL}} when is_list(IL) ->
                           IL;
                       _Val ->
                           error_logger:error_msg
                             ("indexing function returned ~p", [_Val]),
                           []
                   catch
                       _:_ ->
                           error_logger:error_msg
                             ("exception invoking indexing function", []),
                           []
                   end,

               ResultLinks =
                   lists:map(fun({Bucket,Key})  when is_binary(Bucket), is_binary(Key) ->
                                     {{Bucket, Key}, IdxTag};
                                ([Bucket, Key]) when is_binary(Bucket), is_binary(Key) ->
                                     {{Bucket, Key}, IdxTag}
                             end,
                             NewLinksSansTag)
                   ++
                   KeepLinks,

               dict:store(?MD_LINKS, ResultLinks, MDAcc)
       end,
       MD,
       IndexHooks).


%%%%%% code from riak_kv_put_fsm %%%%%%


get_index_hooks(Bucket) ->

    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),

    IndexHooks = proplists:get_value(link_index, BucketProps, []),
    case IndexHooks of
        <<"none">> ->
            {ok, []};
        {struct, Hook} ->
            {ok, [{struct, Hook}]};
        IndexHooks when is_list(IndexHooks) ->
            {ok, IndexHooks};
        V ->
            error_logger:error_msg("bad value in bucket_prop ~p:link_index: ~p", [Bucket,V]),
            {ok, []}
    end.


apply_index_hook({struct, Hook}, MD, Value) ->
    Mod = proplists:get_value(<<"mod">>, Hook),
    Fun = proplists:get_value(<<"fun">>, Hook),
    JSName = proplists:get_value(<<"name">>, Hook),
    invoke_hook(Mod, Fun, JSName, MD, Value);
apply_index_hook(HookDef, _, _) ->
    {error, {invalid_hook_def, HookDef}}.

invoke_hook(Mod0, Fun0, undefined, MD, Value) when Mod0 /= undefined, Fun0 /= undefined ->
    Mod = binary_to_atom(Mod0, utf8),
    Fun = binary_to_atom(Fun0, utf8),
    try
        {erlang, {Mod, Fun}, Mod:Fun(MD, Value)}
    catch
        Class:Exception ->
            {erlang, {Mod, Fun}, {'EXIT', Mod, Fun, Class, Exception}}
    end;
invoke_hook(undefined, undefined, JSName, MD, Value) when JSName /= undefined ->
    {js, JSName, riak_kv_js_manager:blocking_dispatch
     (?JSPOOL_HOOK, {{jsfun, JSName}, [jsonify_metadata(MD), Value]}, 5)};
invoke_hook(_, _, _, _, _) ->
    {error, {invalid_hook_def, no_hook}}.




%%%%% code from riak_object %%%%%%

jsonify_metadata(MD) ->
    MDJS = fun({LastMod, Now={_,_,_}}) ->
                   % convert Now to JS-readable time string
                   {LastMod, list_to_binary(
                               httpd_util:rfc1123_date(
                                 calendar:now_to_local_time(Now)))};
              ({<<"Links">>, Links}) ->
                   {<<"Links">>, [ [B, K, T] || {{B, K}, T} <- Links ]};
              ({Name, List=[_|_]}) ->
                   {Name, jsonify_metadata_list(List)};
              ({Name, Value}) ->
                   {Name, Value}
           end,
    {struct, lists:map(MDJS, dict:to_list(MD))}.

%% @doc convert strings to binaries, and proplists to JSON objects
jsonify_metadata_list([]) -> [];
jsonify_metadata_list(List) ->
    Classifier = fun({Key,_}, Type) when (is_binary(Key) orelse is_list(Key)),
                                         Type /= array, Type /= string ->
                         struct;
                    (C, Type) when is_integer(C), C >= 0, C =< 256,
                                   Type /= array, Type /= struct ->
                         string;
                    (_, _) ->
                         array
                 end,
    case lists:foldl(Classifier, undefined, List) of
        struct -> {struct, [ {if is_list(Key) -> list_to_binary(Key);
                                 true         -> Key
                              end,
                              if is_list(Value) -> jsonify_metadata_list(Value);
                                 true           -> Value
                              end}
                             || {Key, Value} <- List]};
        string -> list_to_binary(List);
        array -> List
    end.

is_updated(O) ->
    M = riak_object:get_update_metadata(O),
    V = riak_object:get_update_value(O),
    case dict:find(clean, M) of
        error -> true;
        {ok,_} ->
            case V of
                undefined -> false;
                _ -> true
            end
    end.
