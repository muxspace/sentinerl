%% Author: Brian Lee Yung Rowe
%% Created: 2011.07.19
-module(sentinerl).
-vsn("1.0.0").
-export([first/1, first/2,
         next/2, next/3,
         last/1, last/2]).


%% Uses 'first' as the checkpoint
first(Name) ->
  Iteration = id_manager:next_id(Name),
  first(Iteration,Name).

first(Iteration, Name) ->
  Checkpoint = first,
  Timestamp = timestamp(),
  Bucket = farm_tools:binarize(Name),
  Key = farm_tools:binarize([Iteration, "-", Checkpoint]),
  O = checkpoint(Name,Iteration,Checkpoint,Timestamp),
  %riak_pool:persist(Bucket, Key, O).
  riak_util:put(Bucket, Key, O),
  Iteration.

next(Name, Checkpoint) ->
  Iteration = id_manager:this_id(Name),
  next(Iteration, Name, Checkpoint).

next(Iteration, Name, Checkpoint) ->
  Timestamp = timestamp(),
  Bucket = farm_tools:binarize(Name),
  Key = farm_tools:binarize([Iteration, "-", Checkpoint]),
  O = checkpoint(Name,Iteration,Checkpoint,Timestamp),
  %riak_pool:persist(Bucket, Key, O).
  riak_util:put(Bucket, Key, O).

%% Uses 'last' as the checkpoint
last(Name) ->
  Iteration = id_manager:this_id(Name),
  last(Iteration,Name).

last(Iteration, Name) ->
  Checkpoint = last,
  Timestamp = timestamp(),
  Bucket = farm_tools:binarize(Name),
  Key = farm_tools:binarize([Iteration, "-", Checkpoint]),
  O = checkpoint(Name,Iteration,Checkpoint,Timestamp),
  %riak_pool:persist(Bucket, Key, O).
  riak_util:put(Bucket, Key, O).


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
