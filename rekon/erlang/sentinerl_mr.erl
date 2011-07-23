-module(sentinerl_mr).
-compile(export_all).

do_job() ->
  {ok, Client} = riak:local_client(),
  Keys = [ {<<"test_3">>,<<"1-a">>}, {<<"test_3">>,<<"1-b">>} ],
  BinToTerm = {map, {modfun, sentinerl_mr,binary_to_term}, none, false},
  PhaseGroup = {reduce, {modfun, sentinerl_mr,group}, none, false},
  PhaseMeasure = {map, {modfun, sentinerl_mr,group}, none, false},
  PhaseAggregate = {reduce, {modfun, sentinerl_mr,group}, none, true},
  Client:mapred(Keys, [BinToTerm, PhaseGroup, PhaseMeasure, PhaseAggregate]).

binary_to_term(RiakObject, _, _) ->
  Raw = riak_object:get_value(RiakObject),
  case is_binary(Raw) of
    true -> Term = binary_to_term(Raw);
    false -> Term = Raw
  end,
  io:format("[sentinerl_mr:binary_to_term] Raw term is~n  ~p~n",[Term]),
  [ Term ].


%% Reduce phase to group checkpoints by name and iteration
%% In (Name):
%%   [ [ {name,Name}, {iteration,Iteration},
%%     {checkpoint,Checkpoint}, {timestamp,Timestamp} ] ]
%% Out:
%%   [ {Iteration, [ {Checkpoint,Timestamp} ] } ]
group(RawList, Arg) -> 
  %List = lists:filter(
  %  fun(X) -> proplists:get_value(name, X) == Arg end, RawList),
  io:format("[sentinerl_mr:group] Input = ~p~n", [RawList]),
  io:format("[sentinerl_mr:group] Arg = ~p~n", [Arg]),
  Fn = fun(X, Acc) -> 
    io:format("[sentinerl_mr:group] X = ~p~n Acc = ~p~n", [X,Acc]),
    Iteration = proplists:get_value(iteration,X),
    Checkpoint = proplists:get_value(checkpoint,X),
    Timestamp = proplists:get_value(timestamp,X),
    case proplists:get_value(Iteration, Acc) of
      undefined ->
        [ {Iteration, [{Checkpoint,Timestamp}]} | Acc ];
      Checkpoints when is_list(Checkpoints) ->
        lists:keyreplace(Iteration,1,Acc,
          {Iteration, [{Checkpoint,Timestamp}|Checkpoints]} );
      Other ->
        io:format("[sentinerl_mr:group] Oops, wasn't expecting '~p'~n", [Other]),
        Acc
    end
  end,
  Out = lists:foldl(Fn, [], RawList),
  io:format("[sentinerl_mr:group] Output = ~p~n", [Out]),
  Out.


%% Map phase to calculate the times between two checkpoints
%% In (CheckpointA, CheckpointB):
%%   [ {Iteration, [ {Checkpoint,Timestamp} ] } ]
%% Out:
%%   [ {Iteration, [ {start,CheckpointA}, {stop,CheckpointB},
%%     {duration,Duration} ]} ]
%measure(RiakObject, _, Arg) ->
measure(O, _, Arg) ->
  io:format("[sentinerl_mr:measure] Input = ~p~n", [O]),
  [CheckpointA, CheckpointB] = case Arg of
    [A,B] -> [A,B];
    _ -> [a,b]
  end,
  Fn = fun(X) ->
    {Iteration, List} = X,
    TimestampA = proplists:get_value(CheckpointA,List),
    TimestampB = proplists:get_value(CheckpointB,List),
    Duration = TimestampB - TimestampA,
    {Iteration, [{start,CheckpointA}, {stop,CheckpointB}, {duration,Duration}] }
  end,
  Out = lists:map(Fn, O),
  io:format("[sentinerl_mr:measure] Output = ~p~n", [Out]),
  Out.


%% Reduce phase to run aggregate statistics on the measurements
%% In:
%%   [ {Iteration, 
%%       [ {start,CheckpointA}, {stop,CheckpointB}, {duration,Duration} ]} ]
%% Out:
%%   [ {start,CheckpointA}, {stop,CheckpointB},
%%     {min,Min}, {max,Max}, {avg,Avg}]
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
