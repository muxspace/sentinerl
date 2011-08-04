-module(id_manager).
-behaviour(gen_server).
-include("private_macros.hrl").
-export([start_link/0, init/1, terminate/2]).
-export([handle_call/3,handle_cast/2,handle_info/2, code_change/3]).
-export([next_id/1, this_id/1, which_ids/0]).


%% This is a tuple list for each id type
%% TODO The last_ids needs to be persisted unless the id has a built-in
%% timestamp component
-record(state, {all_ids=[], last_ids=[]}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% What ids exist?
which_ids() ->
  gen_server:call(node_id(), which_ids).

%% Value of the current id
this_id(What) ->
  gen_server:call(node_id(), {this_id,What}).

next_id(What) ->
  gen_server:call(node_id(), {next_id,What}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
node_id() -> {global, id_manager}.

%% TODO Put this in a separate process to improve performance
%persist({What,Id}, Pid) ->
%  Object = riakc_obj:new(<<"ids">>, farm_tools:binarize([What]), {What,Id}),
%  ?verbose("Putting ~p => ~p~n",[What,Id]),
%  riakc_pb_socket:put(Pid, Object).

%get_ids(Pid) ->
%  ?info("Starting socket client"),
%  %{ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8081),
%  pong = riakc_pb_socket:ping(Pid),
%  {ok, Ks} = riakc_pb_socket:list_keys(Pid, <<"ids">>),
%  Fn = fun(K) ->
%    {ok, O} = riakc_pb_socket:get(Pid, <<"ids">>, K),
%    binary_to_term(riakc_obj:get_value(O))
%  end,
%  lists:map(Fn, Ks).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
  gen_server:start_link(node_id(), ?MODULE, [], []).

init(_) ->
  %process_flag(trap_exit, true),
  %AllIds = riak_pool:retrieve(<<"id.manager">>, <<".index">>),
  %LastIds = [riak_pool:retrieve(<<"id.manager">>,Id) || Id <- AllIds],
  AllIds = riakpool_client:get(<<"id.manager">>, <<".index">>),
  LastIds = [riakpool_client:get(<<"id.manager">>,Id) || Id <- AllIds],
  {ok, #state{last_ids=LastIds, all_ids=AllIds}}.

% Persisting here isn't working
terminate(_Reason, _State) ->
  ok.

handle_call(which_ids, _From, #state{all_ids=Ids}=State) ->
  {reply, Ids, State};
  
handle_call({this_id,What}, _From, State) ->
  Tuple = lists:keyfind(What,1, State#state.last_ids),
  Id = case Tuple of
    {_,V} -> V;
    _ -> not_found
  end,
  {reply, Id, State};

handle_call({next_id,What}, _From, State) ->
  Tuple = lists:keyfind(What,1, State#state.last_ids),
  case Tuple of
    {_,LastId} ->
      Id = LastId + 1,
      LastIds = lists:keyreplace(What,1, State#state.last_ids, {What,Id});
    _ -> Id = 1,
      AllIds = [What|State#state.all_ids],
      %riak_pool:persist(<<"id.manager">>, <<".all_ids">>, AllIds),
      riakpool:persist(<<"id.manager">>, <<".index">>, AllIds),
      LastIds = [{What,Id} | State#state.last_ids]
  end,
  %riak_pool:persist(<<"id.manager">>, farm_tools:binarize([What]), {What,Id}),
  riakpool:put(<<"id.manager">>, farm_tools:binarize([What]), {What,Id}),
  %?persist({What,NewId}, State#state.riak_pid),
  {reply, Id, State#state{last_ids=LastIds}};

handle_call(stop, _From, State) ->
  {stop, normal, ok, State}.

handle_cast(stop, State) ->
  {stop, normal, State}.

handle_info(_Message, State) ->
  {noreply, State}.

code_change(_OldVsn, Session, _Extra) ->
  {ok, Session}.

