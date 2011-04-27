-module(mock_kv).

-export([with_mock_store/5]).

%%%==================== Mock store stuff:
create_mock_store(Nr, ClientID, Bucket, MapRedDelay, Contents) when is_integer(Nr) ->
    Table1 = ets:new(content,[public]),
    Table2 = ets:new(meta,[public]),
    Instance = mock_kv_store:new(ClientID, Table1, Table2, Bucket, MapRedDelay),
    Instance:init(Contents),
    Instance.

with_mock_store(Nr, Bucket, Data, Delay, Body) when is_function(Body,1) ->
    ClientID = list_to_binary("peer-"++integer_to_list(Nr)),
    MockStore = create_mock_store(Nr, ClientID, Bucket, Delay, Data),
    try Body(MockStore)
    catch
        Class:Reason ->
            All = ets:tab2list(MockStore:content_table()),
            error_logger:error_msg("Failed with ~p:~p~nStore= ~p", [Class,Reason,All]),
            erlang:raise(Class,Reason,erlang:get_stacktrace())
    after
	MockStore:stop()
    end.

