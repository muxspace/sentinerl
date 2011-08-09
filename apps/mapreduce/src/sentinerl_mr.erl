-module(sentinerl_mr).
-compile(export_all).

do_job(Bucket) ->
  {ok, Client} = riak:local_client(),
  %Keys = [ {Bucket,<<"1-a">>}, {Bucket,<<"1-b">>} ],
  BinToTerm = {map, {modfun, masseuse,binary_to_term}, none, false},
  PhaseGroup = {reduce, {modfun, sentinerl_mr,group}, none, false},
  ToRiak = {reduce, {modfun, masseuse,as_keydata}, none,false},
  PhaseMeasure = {map, {modfun, sentinerl_mr,measure}, none, false},
  PhaseAggregate = {reduce, {modfun, sentinerl_mr,aggregate}, none, true},
  Client:mapred_bucket(Bucket, [BinToTerm, PhaseGroup, ToRiak, PhaseMeasure, PhaseAggregate]).


fold_group_input({Iteration, Checkpoints}, Acc) when is_list(Checkpoints) ->
  %io:format("[sentinerl_mr:group]~n  Input = ~p~n  Acc = ~p~n", [{Iteration,Checkpoints},Acc]),
  ItNew = case proplists:get_value(Iteration,Acc) of
    undefined -> Checkpoints;
    ItOld -> lists:merge(lists:sort(ItOld),lists:sort(Checkpoints))
  end,
  lists:keystore(Iteration,1, Acc, {Iteration,ItNew});
  
fold_group_input(Input, Acc) when is_list(Input) ->
  %io:format("[sentinerl_mr:group]~n  Input = ~p~n  Acc = ~p~n", [Input,Acc]),
  Iteration = proplists:get_value(iteration,Input),
  Checkpoint = proplists:get_value(checkpoint,Input),
  Timestamp = proplists:get_value(timestamp,Input),
  Next = case proplists:get_value(Iteration, Acc) of
    undefined ->
      [ {Iteration, [{Checkpoint,Timestamp}]} | Acc ];
    Checkpoints when is_list(Checkpoints) ->
      lists:keyreplace(Iteration,1,Acc,
        {Iteration, [{Checkpoint,Timestamp}|Checkpoints]} );
    Other ->
      io:format("[sentinerl_mr:group] Oops, wasn't expecting '~p'~n", [Other]),
      Acc
  end,
  %io:format("[sentinerl_mr:group]~n  Next = ~p~n", [Next]),
  Next.
  


%% Reduce phase to group checkpoints by name and iteration
%% In (Name):
%%   [ [ {name,Name}, {iteration,Iteration},
%%     {checkpoint,Checkpoint}, {timestamp,Timestamp} ] ]
%% Out:
%%   [ {Iteration, [ {bucket,Bucket} | [{Checkpoint,Timestamp}] ] } ]
group(RawList, _) -> 
  %io:format("[sentinerl_mr:group] Input = ~p~n", [RawList]),
  Fn = fun(X, Acc) -> fold_group_input(X,Acc) end,
  Out = lists:foldl(Fn, [], RawList),
  %io:format("[sentinerl_mr:group] Output = ~p~n", [Out]),
  Out.

%% Map phase to calculate the times between two checkpoints
%% In (CheckpointA, CheckpointB):
%%   [ {Iteration, [ {Checkpoint,Timestamp} ] } ]
%% Out:
%%   [ {Iteration, [ {start,CheckpointA}, {stop,CheckpointB},
%%     {duration,Duration} ]} ]
measure(_RiakObject, {_Buckey,O}, Arg) ->
  %io:format("[sentinerl_mr:measure] Input = ~p~n", [O]),
  [CheckpointA, CheckpointB] = case Arg of
    [A,B] -> [A,B];
    _ -> [first,last]
  end,
  {Iteration, List} = O,
  TimestampA = proplists:get_value(CheckpointA,List, not_found_a),
  TimestampB = proplists:get_value(CheckpointB,List, not_found_b),
  if
    TimestampA == not_found_a orelse TimestampB == not_found_b ->
      Duration = orphaned;
    true ->
      Duration = TimestampB - TimestampA
  end,
  Out = {Iteration,
    [{start,CheckpointA}, {stop,CheckpointB}, {duration,Duration}] },
  %io:format("[sentinerl_mr:measure] Output = ~p~n", [[Out]]),
  [Out].


%% Reduce phase to run aggregate statistics on the measurements
%% In:
%%   [ {Iteration, 
%%       [ {start,CheckpointA}, {stop,CheckpointB}, {duration,Duration} ]} ]
%% Out:
%%   [ {start,CheckpointA}, {stop,CheckpointB},
%%     {min,Min}, {max,Max}, {avg,Avg}]
%[{415,
%  [{start,first},
%   {stop,last},
%   {duration,orphaned}]},
% {604,
%  [{start,first},
%   {stop,last},
%   {duration,orphaned}]}
%]

sample_start_stop([{summary,_V}|T]) -> sample_start_stop(T);

sample_start_stop([{K,V}|_]) when is_integer(K) ->
  Start = proplists:get_value(start,V),
  Stop = proplists:get_value(stop,V),
  {Start,Stop}.

  
fold_agg_input({summary,List1}, []) ->
  %io:format("[fold_agg_input] Seeding Acc~n"),
  [{summary,List1}];
  
fold_agg_input({summary,List1}, [{summary,List0}]) ->
  %io:format("[fold_agg_input] Merging Accs ~n  A:~p~n  B:~p~n", [List0,List1]),
  Dict0 = dict:from_list(List0),
  Dict1 = dict:from_list(List1),
  Fn = fun(K, V0,V1) ->
    case K of
      min -> min(V0,V1);
      max -> max(V0,V1);
      avg ->
        C0 = dict:fetch(count,Dict0),
        C1 = dict:fetch(count,Dict1),
        (V0 * C0  + V1 * C1) / (C0 + C1);
      count -> V0 + V1;
      orphans -> V0 + V1;
      _ -> V0 % Anything else should match
    end
  end,
  Summary = [ {summary, dict:to_list(dict:merge(Fn, Dict0, Dict1))} ],
  %io:format("[fold_agg_input] Summary = ~p~n",[Summary]),
  Summary;
  
fold_agg_input({Index,List1}, []) when is_integer(Index) ->
  fold_agg_input({Index,List1}, [{summary,[]}]);

fold_agg_input({Index,List1}, [{summary,Acc0}]) when is_integer(Index) ->
  %io:format("Checkpoint = ~p~nAcc0 = ~p~n", [List1,Acc0]),
  Duration = proplists:get_value(duration,List1),
  Count = proplists:get_value(count,Acc0,0),
  OCount = proplists:get_value(orphans,Acc0,0),

  %io:format("[update_stats] Duration = ~p~n",[Duration]),
  Acc1 = case Count + OCount of
    0 ->
      [{min,Duration},{max,Duration},{avg,Duration}, {count,0},{orphans,0}];
    _ ->
      update_stats(Duration, Acc0)
  end,
  Acc2 = case Duration of
    orphaned -> increment(orphans,Acc1);
    _ -> increment(count,Acc1)
  end,
  % [{min,Duration},{max,Duration},{avg,Duration},{count,Count},{orphans,Os}];
  %io:format("[update_stats] Acc2 = ~p~n",[Acc2]),
  [{summary,Acc2}];

fold_agg_input(Other, Acc) ->
  io:format("[fold_agg_input] Unexpected:~n  ~p~n  ~p~n",[Other,Acc]),
  Acc.
  
aggregate([], _Arg) -> [];
aggregate(List, _Arg) when is_list(List) ->
  %io:format("[sentinerl_mr:aggregate] Input = ~p~n", [List]),

  [{summary,Out0}] = lists:foldl(fun fold_agg_input/2, [], List),
  case proplists:get_value(start,Out0) of
    undefined ->
      {Start,Stop} = sample_start_stop(lists:reverse(List)),
      %io:format("[sentinerl_mr:aggregate] Sample {~p,~p}~n",[Start,Stop]),
      Out1 = [{start,Start},{stop,Stop}] ++ Out0;
    _ -> 
      Out1 = Out0
  end,
  %io:format("[sentinerl_mr:aggregate] Output = ~p~n", [Out1]),
  [{summary,Out1}].


increment(Name,Stats) ->
  Count = proplists:get_value(Name,Stats,0),
  lists:keystore(Name,1,Stats, {Name,Count+1}).

update_stats(Duration,Stats) ->
  case Duration of
    orphaned -> Stats;
    _ -> update_stats(Duration,Stats,[lmin,lmax,lavg])
  end.

update_stats(_Duration,Stats,[]) -> Stats;

update_stats(Duration,Stats0,[H|T]) ->
  %io:format("[update_stats] Calculating ~p~n", [H]),
  Stats1 = {sentinerl_mr,H}(Duration,Stats0),
  %io:format("[update_stats] Stats: ~p~n", [Stats1]),
  update_stats(Duration, Stats1, T).

lmin(Duration,P) ->
  Old = proplists:get_value(min,P),
  if 
    Old == orphaned -> lists:keyreplace(min,1,P, {min,Duration});
    Duration < Old -> lists:keyreplace(min,1,P, {min,Duration});
    true -> P
  end.

lmax(Duration,P) ->
  Old = proplists:get_value(max,P),
  if 
    Old == orphaned -> lists:keyreplace(max,1,P, {max,Duration});
    Duration > Old -> lists:keyreplace(max,1,P, {max,Duration});
    true -> P
  end.

lavg(Duration,P) ->
  case proplists:get_value(avg,P) of
    orphaned ->
      lists:keyreplace(avg,1,P, {avg,Duration});
    OldAvg ->
      N = proplists:get_value(count,P,0),
      NewAvg = (N * OldAvg + Duration) / (N+1),
      lists:keyreplace(avg,1,P, {avg,NewAvg})
  end.

