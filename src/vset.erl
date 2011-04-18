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

%%@doc
%%  A vset is a temportal set abstraction, in which each add/remove
%%  operation is augmented with a vclock timestamp.  Thus, it allows
%%  reordering (in physical time) of add and remove operation, while
%%  leaving only the vclock-time scale observable, and specifically
%%  it allows merging of concurrent updates in an orderly fashion.
%%@end

-module(vset).
-author("Kresten Krab Thorup <krab@trifork.com>").

-export([new/0,contains/2,add/3,remove/3,merge/1,merge/2,values/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

new() ->
    dict:new().

values(VSet) ->
    dict:fold(fun(Value,{_VC,Bools},Acc) ->
                      case lists:any(fun(Bool) -> Bool end, Bools) of
                          true -> [Value|Acc];
                          false -> Acc
                      end
              end,
              [],
              VSet).

contains(Value,VSet) ->
    lists:any(fun(Bools) -> lists:any(fun(Bool) -> Bool end, Bools) end,
              case dict:find(Value, VSet) of
                  error -> [];
                  {ok, {_VC,Bools}} -> Bools
              end).


value_merge({VC1,V1}=O1,{VC2,V2}=O2) ->
    case vclock:descends(VC1,VC2) of
        true ->
            O1;
        false ->
            case vclock:descends(VC2,VC1) of
                true ->
                    O2;
                false ->
                    {vclock:merge(VC1,VC2), lists:usort(V1 ++ V2)}
            end
    end.

merge(VSet1,VSet2) ->
    dict:merge(fun value_merge/2, VSet1, VSet2).

merge([]) ->
    [];
merge([S1]) ->
    S1;
merge([S1|Rest]) ->
    lists:fold(fun(Set1,Set2) -> merge(Set1,Set2) end,
               S1,
               Rest).

get_vclock(Value,VSet) ->
    case dict:find(Value,VSet) of
        error ->
            vclock:fresh();
        {ok, {VC,_}} ->
            VC
    end.

add(Value,ClientID,VSet) ->
    VClock = get_vclock (Value, VSet),
    VC2 = vclock:increment(ClientID,VClock),
    dict:store(Value,{VC2,[true]}, VSet).

remove(Value,ClientID,VSet) ->
    VClock = get_vclock (Value, VSet),
    VC2 = vclock:increment(ClientID,VClock),
    dict:store(Value,{VC2,[false]}, VSet).



-ifdef(TEST).

simple_test() ->
    VC1 = [{jens,{1,1}}],
    VC2 = [{jens,{2,1}}],

    S1 = vset:new(),
    S2 = vset:add(foo, VC1, S1),
    [foo] = values(S2),

    S3 = vset:remove(foo, VC2, S2),
    [] = values(S3)
    .

simple2_test() ->
    VC1 = [{jens,{1,1}}],
    VC2 = [{jens,{2,1}}],

    S1 = vset:new(),

    S2 = vset:remove(foo, VC2, S1),
    [] = values(S2),

    S3 = vset:add(foo, VC1, S2),
    [] = values(S3)
    .



-endif.

