-module(sentinerl_mr_tests).
-include_lib("eunit/include/eunit.hrl").

binary_to_term_test_() ->
  O = [ {key1, value1}, {key2, value2}, {key3, value3} ],
  R = riak_object:new(<<"bucket">>, <<"key">>, O),
  Act = sentinerl_mr:binary_to_term(R, undefined, none),
  ?_assertEqual([O], Act).

reduce_to_keydata_test_() ->
  O1 = {1, [{bucket,<<"temp">>}, {first,10}, {b,12}, {c,14}]},
  O2 = {2, [{bucket,<<"temp">>}, {last,11}, {b,13}, {c,15}]},
  R1 = {{<<"temp">>,<<"1-first">>}, O1},
  R2 = {{<<"temp">>,<<"2-first">>}, O2},
  Exp = [R1,R2],
  Act = sentinerl_mr:reduce_to_keydata([O1,O2],none),
  ?_assertEqual(Exp, Act).

group_test_() ->
  O = [ [{name,test_3},
         {iteration,1},
         {checkpoint,first},
         {timestamp,15}],
        [{name,test_3},
         {iteration,1},
         {checkpoint,b},
         {timestamp,20}],
        [{name,test_3},
         {iteration,1},
         {checkpoint,last},
         {timestamp,25}] ],
  Exp = [ {1, [{last,25}, {b,20}, {first,15}, {bucket,<<"bucket">>}] } ],
  Act = sentinerl_mr:group(O, <<"bucket">>),
  ?_assertEqual(Exp, Act).

measure_test_() ->
  Tb = 20,
  Ta = 15,
  O = {1, [{last,Tb}, {b,18}, {first,Ta}] },
  Exp = [ {1, [{start,first}, {stop,last}, {duration, Tb-Ta}]} ],
  Act = sentinerl_mr:measure(dummy_object, O, none),
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


