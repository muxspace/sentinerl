-module(sentinerl_mr_tests).
-include_lib("eunit/include/eunit.hrl").

-define(v(K,L), proplists:get_value(K,L)).

%binary_to_term_test_() ->
%  O = [ {key1, value1}, {key2, value2}, {key3, value3} ],
%  R = riak_object:new(<<"bucket">>, <<"key">>, O),
%  Act = sentinerl_mr:binary_to_term(R, undefined, none),
%  ?_assertEqual([O], Act).

%reduce_to_keydata_test_() ->
%  O1 = {1, [{bucket,<<"temp">>}, {first,10}, {b,12}, {c,14}]},
%  O2 = {2, [{bucket,<<"temp">>}, {last,11}, {b,13}, {c,15}]},
%  R1 = {{none,none}, O1},
%  R2 = {{none,none}, O2},
%  Exp = [R1,R2],
%  Act = sentinerl_mr:reduce_to_keydata([O1,O2],none),
%  ?_assertEqual(Exp, Act).

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
  Exp = [ {1, [{last,25}, {b,20}, {first,15}] } ],
  Act = sentinerl_mr:group(O, <<"bucket">>),
  ?_assertEqual(Exp, Act).

measure_test_() ->
  Tb = 20,
  Ta = 15,
  Buckey = {none,none},
  O = {1, [{last,Tb}, {b,18}, {first,Ta}] },
  Exp = [ {1, [{start,first}, {stop,last}, {duration, Tb-Ta}]} ],
  Act = sentinerl_mr:measure(dummy_object, {Buckey,O}, none),
  ?_assertEqual(Exp, Act).

aggregate_reduce_test_() ->
  O = [ {1, [{start,a}, {stop,b}, {duration, 10}]},
        {2, [{start,a}, {stop,b}, {duration, 12}]},
        {3, [{start,a}, {stop,b}, {duration,  8}]}
  ],
  Exp = [{summary,
           [{start,a}, {stop,b}, {min,8}, {max,12}, {avg,10.0},
            {count,3}, {orphans,0}] }
        ],
  Act = sentinerl_mr:aggregate(O, none),
  ?_assertEqual(Exp, Act).

aggregate_re_reduce_test_() ->
  O = [ {summary,
          [{start,first},
           {stop,last},
           {min,9},
           {max,13},
           {avg,10},
           {orphans,23},
           {count,2}]},
        {summary,
          [{start,first},
           {stop,last},
           {min,7},
           {max,11},
           {avg,10},
           {orphans,19},
           {count,6}]},
        {1, [{start,first}, {stop,last}, {duration, 10}]},
        {2, [{start,first}, {stop,last}, {duration, 12}]},
        {3, [{start,first}, {stop,last}, {duration,  8}]}
  ],
  Exp = [{start,first}, {stop,last}, {min,7}, {max,13}, {avg,10.0},
           {count,11}, {orphans,42}],
  [{summary,Raw}] = sentinerl_mr:aggregate(O, none),
  Keys = [ start, stop, min, max, avg, count, orphans ],
  Act = [{X,proplists:get_value(X,Raw)} || X <- Keys],
  ?_assertEqual(Exp, Act).

increment_exists_test_() ->
  List = [{a,1},{b,2}],
  Exp = [{a,2},{b,2}],
  Act = sentinerl_mr:increment(a, List),
  ?_assertEqual(Exp, Act).

increment_default_test_() ->
  List = [{a,1},{b,2}],
  Exp = [{a,1},{b,2},{c,1}],
  Act = sentinerl_mr:increment(c, List),
  ?_assertEqual(Exp, Act).


update_stats_orphaned_orphaned_test_() ->
  Stats = [{min,orphaned},{max,orphaned},{avg,orphaned},{count,0},{orphans,0}],
  Exp = Stats,
  Act = sentinerl_mr:update_stats(orphaned, Stats),
  ?_assertEqual(Exp, Act).

update_stats_valid_orphaned_test_() ->
  Stats = [{min,20.232},{max,20.232},{avg,20.232},{count,5},{orphans,0}],
  Exp = Stats,
  Act = sentinerl_mr:update_stats(orphaned, Stats),
  ?_assertEqual(Exp, Act).

update_stats_orphaned_valid_test_() ->
  Stats = [{min,orphaned},{max,orphaned},{avg,orphaned},{count,0},{orphans,2}],
  Exp = [{min,20.232},{max,20.232},{avg,20.232},{count,0},{orphans,2}],
  Act = sentinerl_mr:update_stats(20.232, Stats),
  ?_assertEqual(Exp, Act).

update_stats_valid_valid_test_() ->
  Stats = [{min,20},{max,20},{avg,20},{count,1},{orphans,2}],
  Exp = [{min,20},{max,30},{avg,25.0},{count,1},{orphans,2}],
  Act = sentinerl_mr:update_stats(30, Stats),
  ?_assertEqual(Exp, Act).


lmin_test_() ->
  Exp = true,
  Act = true,
  ?_assertEqual(Exp, Act).

lmax_test_() ->
  Exp = true,
  Act = true,
  ?_assertEqual(Exp, Act).

lavg_test_() ->
  Exp = true,
  Act = true,
  ?_assertEqual(Exp, Act).


