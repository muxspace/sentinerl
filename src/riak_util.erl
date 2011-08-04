%% Author: Brian Lee Yung Rowe
%% Created: 2011.07.19
-module(riak_util).
-include("private_macros.hrl").
-compile(export_all).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
put(B,K,O) ->
  riakpool_client:put(B, K, O).

get(B,K) ->
  {ok,Binary} = riakpool_client:get(B, K),
  binary_to_term(Binary).


new_connection() ->
  {ok, App} = application:get_application(),
  new_connection(App).

new_connection(App) ->
  ?verbose("Using application ~p", [App]),
  riakc_pb_socket:start_link(get_host(App),get_port(App),get_options()).



get_host() ->
  {ok, App} = application:get_application(),
  get_host(App).

get_host(App) ->
 case application:get_env(App, riak_host) of
    undefined -> ?info("Using default host 127.0.0.1"), "127.0.0.1";
    {ok,H} -> H
  end.

get_port() ->
  {ok, App} = application:get_application(),
  get_port(App).

get_port(App) ->
  case application:get_env(App, riak_port) of
    undefined -> ?info("Using default PB port 8081"), 8081;
    {ok,H} -> H
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_options() ->
  [ {queue_if_disconnected,true} ].


