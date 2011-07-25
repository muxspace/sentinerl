%% Author: Brian Lee Yung Rowe
%% Created: 2011.07.19
-module(sentinerl).
-vsn("1.0.0").
-export([first/1,
         next/2,
         last/1]).

%% Uses 'first' as the checkpoint
first(Name) ->
  Checkpoint = first,
  Timestamp = timestamp(),
  Iteration = id_manager:next_id(Name),
  Bucket = farm_tools:binarize(Name),
  Key = farm_tools:binarize([Iteration, "-", Checkpoint]),
  O = checkpoint(Name,Iteration,Checkpoint,Timestamp),
  riak_pool:persist(Bucket, Key, O).

next(Name, Checkpoint) ->
  Timestamp = timestamp(),
  Iteration = id_manager:this_id(Name),
  Bucket = farm_tools:binarize(Name),
  Key = farm_tools:binarize([Iteration, "-", Checkpoint]),
  O = checkpoint(Name,Iteration,Checkpoint,Timestamp),
  riak_pool:persist(Bucket, Key, O).

%% Uses 'last' as the checkpoint
last(Name) ->
  Checkpoint = last,
  Timestamp = timestamp(),
  Iteration = id_manager:this_id(Name),
  Bucket = farm_tools:binarize(Name),
  Key = farm_tools:binarize([Iteration, "-", Checkpoint]),
  O = checkpoint(Name,Iteration,Checkpoint,Timestamp),
  riak_pool:persist(Bucket, Key, O).


%% Returns milliseconds
-spec timestamp() -> number().
timestamp() ->
  {Mega, Sec, Micro} = now(),
  Mega * 1000000000 + Sec * 1000 + Micro/1000.
  
checkpoint(Name, Iteration, Checkpoint, Timestamp) ->
  [ {name,Name},
    {iteration,Iteration},
    {checkpoint,Checkpoint},
    {timestamp,Timestamp} ].
