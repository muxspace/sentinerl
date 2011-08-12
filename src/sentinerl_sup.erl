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
    {ok,App} = application:get_application(),
    riakpool:start_pool(sentinerl_riak_util:get_host(App),
                        sentinerl_riak_util:get_port(App)),

    %RiakPool = sup_util:config(riak_pool, riak_pool, []),
    IdManager = sentinerl_sup_util:config(
      sentinerl_id_manager, sentinerl_id_manager,[]),

    %{ok, { {one_for_one, 5, 10}, [RiakPool, IdManager]} }.
    {ok, { {one_for_one, 5, 10}, [IdManager]} }.

