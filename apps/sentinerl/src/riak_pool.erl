%% Author: Brian Lee Yung Rowe
%% Created: 2011.07.19
-module(riak_pool).
-behaviour(gen_server).
-include("private_macros.hrl").
-compile(export_all).
-record(state, {pool=[]}).
-export([start_link/0, init/1, terminate/2, code_change/3,
         handle_cast/2, handle_call/3, handle_info/2]).
-export([persist/3, retrieve/2, retrieve/1,
         new_connection/0, new_connection/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
persist(Bucket, Key, Object) ->
  gen_server:cast(?MODULE, {persist, {Bucket,Key,Object}}).

retrieve(Bucket, Key) -> 
  gen_server:call(?MODULE, {retrieve, {Bucket,Key}}).

retrieve(Bucket) ->
  gen_server:call(?MODULE, {retrieve, Bucket}).

new_connection() ->
  {ok, App} = application:get_application(),
  new_connection(App).

new_connection(App) ->
  ?verbose("Using application ~p", [App]),
  riakc_pb_socket:start_link(get_host(App),get_port(App),get_options()).

start_link() ->
  gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_host(App) ->
 case application:get_env(App, riak_host) of
    undefined -> ?info("Using default host 127.0.0.1"), "127.0.0.1";
    {ok,H} -> H
  end.

get_port(App) ->
  case application:get_env(App, riak_port) of
    undefined -> ?info("Using default PB port 8081"), 8081;
    {ok,H} -> H
  end.

get_options() ->
  [ {queue_if_disconnected,true} ].


create_pool() ->
  case application:get_env(?MODULE, pool_size) of
    undefined -> ?info("Using default pool size of 6"), V = 6;
    {ok,V} -> V
  end,
  RawPids = lists:map(fun(_) -> new_connection() end, lists:seq(1,V)),
  [ Pid || {ok, Pid} <- RawPids ].

pool_connection(OldPool) ->
  % Get connection from pool
  Count = length(OldPool),
  Idx = random:uniform(Count),
  MaybePid = lists:nth(Idx,OldPool),
  % Check if it's live
  case riakc_pb_socket:ping(MaybePid) of
    pong ->
      Pid = MaybePid,
      NewPool = OldPool;
    _ ->
      {ok,Pid} = new_connection(),
      NewPool = [Pid | [ P || P <- OldPool, P /= Pid ]]
  end,
  {ok, Pid, NewPool}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_) ->
  random:seed(now()),
  ?info("Generating pool of riak clients"),
  Pool = create_pool(),
  {ok, #state{pool=Pool}}.

terminate(normal, _State) -> ok;
terminate(_Stack, _ObjectState) -> ok.

handle_cast({persist, {B,K,O}}, #state{}=State) ->
  {ok, Pid, Pool} = pool_connection(State#state.pool),
  RObject = riakc_obj:new(B,K, O),
  riakc_pb_socket:put(Pid, RObject),
  {noreply, State#state{pool=Pool}}.

handle_call({retrieve, {B,K}}, _From, #state{}=State) ->
  {ok, Pid, Pool} = pool_connection(State#state.pool),
  {ok, RObject} = riakc_pb_socket:get(Pid, B,K),
  {reply, riakc_obj:get_value(RObject), State#state{pool=Pool}};

handle_call({retrieve, B}, _From, #state{}=State) ->
  {ok, Pid, Pool} = pool_connection(State#state.pool),
  {ok, Ks} = riakc_pb_socket:list_keys(Pid, B),
  Fn = fun(K) ->
    case riakc_pb_socket:get(Pid, B,K) of
      {ok, O} -> binary_to_term(riakc_obj:get_value(O));
      {error,notfound} -> []
    end
  end,
  {reply, lists:map(Fn,Ks), State#state{pool=Pool}}.

handle_info({'EXIT', _Pid, _Reason}, #state{}=State) ->
  {noreply, State}.

code_change(_OldVersion, State, _Extra) -> {ok, State}.
