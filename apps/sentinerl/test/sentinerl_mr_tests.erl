-module(sentinerl_mr_tests).
-include_lib("eunit/include/eunit.hrl").

binary_to_term_test_() ->
  O = [ {key1, value1}, {key2, value2}, {key3, value3} ],
  R = riak_object:new(<<"bucket">>, <<"key">>, O),
  Act = sentinerl_mr:binary_to_term(R, undefined, none),
  ?_assertEqual([O], Act).

group_test_() ->
  O = [ [{name,test_3},
         {iteration,1},
         {checkpoint,a},
         {timestamp,15}],
        [{name,test_3},
         {iteration,1},
         {checkpoint,b},
         {timestamp,20}] ],
  Exp = [ {1, [{b,20}, {a,15}] } ],
  Act = sentinerl_mr:group(O, none),
  ?_assertEqual(Exp, Act).

measure_test_() ->
  Tb = 20,
  Ta = 15,
  O = [ {1, [{b,Tb}, {a,Ta}] } ],
  Exp = [ {1, [{start,a}, {stop,b}, {duration, Tb-Ta}]} ],
  Act = sentinerl_mr:measure(O, undefined, none),
  ?_assertEqual(Exp, Act).

aggregate_test_() ->
  O = [ {1, [{start,a}, {stop,b}, {duration, 10}]},
        {2, [{start,a}, {stop,b}, {duration, 12}]},
        {3, [{start,a}, {stop,b}, {duration,  8}]}
  ],
  Exp = [ {start,a}, {stop,b}, {min,8}, {max,12}, {avg,10.0}, {count,3} ],
  Act = sentinerl_mr:aggregate(O, none),
  ?_assertEqual(Exp, Act).

increment_test_() ->
  Exp = true, Act = true,
  ?_assertEqual(Exp, Act).


update_stats_test_() ->
  Exp = true, Act = true,
  ?_assertEqual(Exp, Act).


min_test_() ->
  Exp = true, Act = true,
  ?_assertEqual(Exp, Act).

max_test_() ->
  Exp = true, Act = true,
  ?_assertEqual(Exp, Act).

avg_test_() ->
  Exp = true, Act = true,
  ?_assertEqual(Exp, Act).


