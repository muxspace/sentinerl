-module(sentinerl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    application:start(sasl),
    application:start(riakpool),
    riakpool:start_pool(riak_util:get_host(), riak_util:get_port()),

    %RiakPool = sup_util:config(riak_pool, riak_pool, []),
    IdManager = sup_util:config(id_manager, id_manager,[]),

    %{ok, { {one_for_one, 5, 10}, [RiakPool, IdManager]} }.
    {ok, { {one_for_one, 5, 10}, [IdManager]} }.

