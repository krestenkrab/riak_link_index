-module(riak_column_tests).

-include_lib("eunit/include/eunit.hrl").


simple_test() ->
    mock_kv:with_mock_store
        (1, <<"buck">>, [], 0,
         fun(Client) ->
                 Column = riak_column:new(Client, <<"buck">>, <<"age">>),
                 Column:add(<<"peter1">>, 1),
                 Column:add(<<"peter2">>, 2),
                 Column:add(<<"peter3">>, 3),
                 Column:add(<<"peter4">>, 4),
                 Column:add(<<"peter5">>, 5),
                 Column:add(<<"peter5">>, 6),
                 Column:add(<<"peter6">>, 6),
                 Column:add(<<"peter7">>, 7),
                 Column:add(<<"peter8">>, 8),
                 Column:add(<<"peter9">>, 9),
                 {ok, {_,[3]}} = Column:lookup(<<"peter3">>),

                 {ok, {VClock, [6,5]}} = Column:lookup(<<"peter5">>),
                 ok = Column:put(<<"peter5">>, {VClock, [5]}),
                 {ok, {_, [5]}} = Column:lookup(<<"peter5">>),

                 Values = Column:fold(fun({_Key,{_VC,[V]}}, Acc) -> [V|Acc] end, []),
                 [1,2,3,4,5,6,7,8,9] = lists:sort(Values),

                 All = ets:tab2list(Client:content_table()),
                 error_logger:info_msg("Store= ~p", [All]),

                 ok
         end).

