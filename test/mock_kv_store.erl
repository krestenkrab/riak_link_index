-module(mock_kv_store, [ClientID, ContentTable, MetaTable, MainBucket, MapRedDelay]).

-export([get/2, put/1, put/5, get_bucket/1, set_bucket/2, mapred_bucket_stream/3, get_client_id/0, content_table/0]).

-export([init/1, stop/0]).

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).
-export([assertEquals/1,get_contents/0]).
-endif.

init(Contents) ->
    ets:insert(MetaTable, {{bucket_props, MainBucket}, []}),

    lists:foreach(fun(Obj) ->
			  TabKey = {MainBucket, riak_object:key(Obj)},
			  ets:insert(ContentTable, {TabKey, Obj})
		  end,
		  Contents),
    ok.

content_table() ->
    ContentTable.

get_client_id() ->
    ClientID.

stop() ->
    ets:delete(ContentTable),
    ets:delete(MetaTable),
    ok.

get_bucket(Bucket) ->
    ets:lookup_element(MetaTable, {bucket_props, Bucket}, 2).

set_bucket(Bucket, NewProps) ->
    OldProps = get_bucket(Bucket),
    SumProps = lists:ukeymerge(1,
			       lists:ukeysort(1, NewProps),
			       lists:ukeysort(1, OldProps)),
    ets:insert(MetaTable, {{bucket_props, Bucket}, SumProps}),
    ok.

get(Bucket, Key) ->
    case ets:lookup(ContentTable, {Bucket,Key}) of
	[]     -> {error, notfound};
	[{_,Obj}] -> {ok, Obj}
    end.

put(Obj) ->
    put (Obj, 1, 1, 1, []).

put(Obj,_W,_DW,_TimeOut,Options) ->
    Bucket = riak_object:bucket(Obj),
    Key = riak_object:key(Obj),

%    error_logger:info_msg("putting   ~p", [Obj]),

    Updated = case is_updated(Obj) of
                  true -> riak_object:increment_vclock(riak_object:apply_updates(Obj), ClientID);
                  false -> Obj
              end,

    case ets:lookup(ContentTable, {Bucket,Key}) of
        [] ->
            Merged = Updated;
        [{_,OrigObj}] ->
            Merged = riak_object:reconcile([OrigObj,Updated], true)
    end,

%    error_logger:info_msg("storing ~p", [{{Bucket,Key}, Merged}]),

    ets:insert(ContentTable, {{Bucket,Key}, Merged}),

    case proplists:get_bool(returnbody, Options) of
        true ->
            {ok, Merged};
        false ->
            ok
    end.



mapred_bucket_stream(Bucket, Query, ClientPid) ->
    Ref = make_ref(),
    spawn_link(fun() -> do_mapred_bucket_stream(Bucket, Query, ClientPid, MapRedDelay, Ref) end),
    {ok, Ref}.

do_mapred_bucket_stream(Bucket, Query, ClientPid, MapRedDelay, Ref) ->
    [{map, F, none, true}] = Query,
    ets:foldl(fun({{ObjBucket, _}, Obj}, _) ->
		      if ObjBucket =:= Bucket ->
			      timer:sleep(MapRedDelay),
			      MapResult = xapply(F, [Obj, dummyKeyData, dummyAction]),
			      lists:foreach(fun(Res) ->
						    ClientPid ! {flow_results, dummyPhaseID, Ref, Res}
					    end,
					    MapResult);
			 true ->
			      ok
		      end
	      end,
	      dummy,
	      ContentTable),
    ClientPid ! {flow_results, Ref, done}.

xapply({modfun, Module, Function}, Args) ->
    apply(Module, Function, Args);
xapply({'fun', Fun}, Args) ->
    apply(Fun, Args).

-ifdef(TEST).

assertEquals(OtherPID) ->
    HisObjects = OtherPID:get_contents(),
    MyObjects  = get_contents(),

    length(HisObjects) == length(MyObjects).


get_contents() ->
    mapred_bucket_stream(MainBucket,
                         [{map, {'fun', fun(Obj,_,_) -> [Obj] end}, none, true}],
                         self()),
    get_flow_contents([]).

get_flow_contents(Result) ->
    receive
        {flow_results, _, _, Obj} ->
            get_flow_contents([Obj | Result]);
        {flow_results, _, done} ->
            Result
    end.


-endif.

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
