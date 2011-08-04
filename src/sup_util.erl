-module(sup_util).
-export([perm_spec/3, temp_spec/3, config/3]).

%% This is deprecated. Use perm_spec instead
config(Handle, Module, ServerArgs) -> perm_spec(Handle,Module,ServerArgs).

perm_spec(Handle, Module, ServerArgs) ->
  {Handle,
    {Module, start_link, ServerArgs},
    permanent, brutal_kill, worker, [Module]}.

temp_spec(Handle, Module, ServerArgs) ->
  {Handle,
    {Module, start_link, ServerArgs},
    temporary, brutal_kill, worker, [Module]}.


