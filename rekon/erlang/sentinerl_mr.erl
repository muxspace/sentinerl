-module(sentinerl_mr).
-compile(export_all).

do_job(Bucket) ->
  {ok, Client} = riak:local_client(),
  %Keys = [ {Bucket,<<"1-a">>}, {Bucket,<<"1-b">>} ],
  BinToTerm = {map, {modfun, sentinerl_mr,binary_to_term}, none, false},
  PhaseGroup = {reduce, {modfun, sentinerl_mr,group}, Bucket, false},
  ToRiak = {reduce, {modfun,sentinerl_mr,reduce_to_keydata}, none,false},
  PhaseMeasure = {map, {modfun, sentinerl_mr,measure}, none, false},
  PhaseAggregate = {reduce, {modfun, sentinerl_mr,aggregate}, none, true},
  Client:mapred_bucket(Bucket, [BinToTerm, PhaseGroup, ToRiak, PhaseMeasure, PhaseAggregate]).

binary_to_term(RiakObject, _, _) ->
  Raw = riak_object:get_value(RiakObject),
  case is_binary(Raw) of
    true -> Term = binary_to_term(Raw);
    false -> Term = Raw
  end,
  io:format("[sentinerl_mr:binary_to_term] Raw term is~n  ~p~n",[Term]),
  [ Term ].


%% Reduce phase that takes a list output and creates a list of riak input
%% tuples for subsequent map phases
reduce_to_keydata(List, _Arg) ->
  io:format("[sentinerl_mr:reduce_to_keydata] Input = ~p~n", [List]),
  Checkpoint = first,
  %% TODO: Use Arg as a transform function
  Fn = fun({Iteration,ItList}) ->
    Bucket = proplists:get_value(bucket,ItList),
    Key = list_to_binary(lists:concat([Iteration,"-",Checkpoint])),
    {{Bucket,Key},{Iteration,ItList}}
  end,
  Out = lists:map(Fn,List),
  io:format("[sentinerl_mr:reduce_to_keydata] Output = ~p~n", [Out]),
  Out.


fold_group_input({Iteration, Checkpoints}, Acc, _) when is_list(Checkpoints) ->
  io:format("[sentinerl_mr:group]~n  Input = ~p~n  Acc = ~p~n", [{Iteration,Checkpoints},Acc]),
  ItNew = case proplists:get_value(Iteration,Acc) of
    undefined -> Checkpoints;
    ItOld -> lists:merge(lists:sort(ItOld),lists:sort(Checkpoints))
  end,
  lists:keystore(Iteration,1, Acc, {Iteration,ItNew});
  
fold_group_input(Input, Acc, Bucket) when is_list(Input) ->
  io:format("[sentinerl_mr:group]~n  Input = ~p~n  Acc = ~p~n", [Input,Acc]),
  Iteration = proplists:get_value(iteration,Input),
  Checkpoint = proplists:get_value(checkpoint,Input),
  Timestamp = proplists:get_value(timestamp,Input),
  Next = case proplists:get_value(Iteration, Acc) of
    undefined ->
      [ {Iteration, [{Checkpoint,Timestamp}, {bucket,Bucket}]} | Acc ];
    Checkpoints when is_list(Checkpoints) ->
      lists:keyreplace(Iteration,1,Acc,
        {Iteration, [{Checkpoint,Timestamp}|Checkpoints]} );
    Other ->
      io:format("[sentinerl_mr:group] Oops, wasn't expecting '~p'~n", [Other]),
      Acc
  end,
  io:format("[sentinerl_mr:group]~n  Next = ~p~n", [Next]),
  Next.
  


%% Reduce phase to group checkpoints by name and iteration
%% In (Name):
%%   [ [ {name,Name}, {iteration,Iteration},
%%     {checkpoint,Checkpoint}, {timestamp,Timestamp} ] ]
%% Out:
%%   [ {Iteration, [ {bucket,Bucket} | [{Checkpoint,Timestamp}] ] } ]
group(RawList, Bucket) -> 
  %List = lists:filter(
  %  fun(X) -> proplists:get_value(name, X) == Arg end, RawList),
  io:format("[sentinerl_mr:group] Input = ~p~n", [RawList]),
  io:format("[sentinerl_mr:group] Bucket = ~p~n", [Bucket]),
  Fn = fun(X, Acc) -> fold_group_input(X,Acc,Bucket) end,
  Out = lists:foldl(Fn, [], RawList),
  io:format("[sentinerl_mr:group] Output = ~p~n", [Out]),
  Out.

%% Map phase to calculate the times between two checkpoints
%% In (CheckpointA, CheckpointB):
%%   [ {Iteration, [ {Checkpoint,Timestamp} ] } ]
%% Out:
%%   [ {Iteration, [ {start,CheckpointA}, {stop,CheckpointB},
%%     {duration,Duration} ]} ]
measure(_RiakObject, O, Arg) ->
  io:format("[sentinerl_mr:measure] Input = ~p~n", [O]),
  [CheckpointA, CheckpointB] = case Arg of
    [A,B] -> [A,B];
    _ -> [first,last]
  end,
  {Iteration, List} = O,
  TimestampA = proplists:get_value(CheckpointA,List, not_found_a),
  TimestampB = proplists:get_value(CheckpointB,List, not_found_b),
  %% TODO Add check to ensure these values exist
  Duration = TimestampB - TimestampA,
  Out = {Iteration,
    [{start,CheckpointA}, {stop,CheckpointB}, {duration,Duration}] },
  [Out].


%% Reduce phase to run aggregate statistics on the measurements
%% In:
%%   [ {Iteration, 
%%       [ {start,CheckpointA}, {stop,CheckpointB}, {duration,Duration} ]} ]
%% Out:
%%   [ {start,CheckpointA}, {stop,CheckpointB},
%%     {min,Min}, {max,Max}, {avg,Avg}]
aggregate([], _Arg) -> [];
aggregate(List, _Arg) ->
  io:format("[sentinerl_mr:aggregate] Input = ~p~n", [List]),
  [{_,Sample}|_] = List,
  Start = proplists:get_value(start,Sample),
  Stop = proplists:get_value(stop,Sample),
  Fn = fun(Tuple, Acc) ->
    P = element(2,Tuple),
    Duration = proplists:get_value(duration,P),
    Count = proplists:get_value(count,Acc,0),
    Next = case Count of
      0 -> [{min,Duration}, {max,Duration}, {avg,Duration}];
      _ -> update_stats(Count, Duration, Acc)
    end,
    increment(Next)
  end,
  Out = lists:foldl(Fn, [], List),
  Out1 = [{start,Start}, {stop,Stop}] ++ Out,
  io:format("[sentinerl_mr:measure] Output = ~p~n", [Out1]),
  Out1.


increment(P) ->
  Count = proplists:get_value(count,P,0),
  lists:keystore(count,1,P, {count,Count+1}).

update_stats(Count,Duration,P) ->
  update_stats(Count,Duration,P,[min,max,avg]).

update_stats(_Count,_Duration,P,[]) -> P;

update_stats(Count,Duration,P0,[H|T]) ->
  P1 = {sentinerl_mr,H}(Duration,P0),
  update_stats(Count, Duration, P1,T).

min(New,P) ->
  Old = proplists:get_value(min,P),
  if 
    New < Old -> lists:keyreplace(min,1,P, {min,New});
    true -> P
  end.

max(New,P) ->
  Old = proplists:get_value(max,P),
  if 
    New > Old -> lists:keyreplace(max,1,P, {max,New});
    true -> P
  end.

avg(Duration,P) ->
  Avg = proplists:get_value(avg,P),
  N = proplists:get_value(count,P),
  New = (N * Avg + Duration) / (N+1),
  lists:keyreplace(avg,1,P, {avg,New}).
