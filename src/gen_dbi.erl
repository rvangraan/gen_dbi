%%--------------------------------------------------------------------------------------------------
-module(gen_dbi).
%%--------------------------------------------------------------------------------------------------
-export([
  connect/0,
  disconnect/1,
  execute/1,
  execute/2,
  select_proplists/1,
  select_proplists/2
]).
%%--------------------------------------------------------------------------------------------------

connect() ->
  Params = application:get_all_env(osw_dbi),

  Host       = proplists:get_value(host,     Params, "127.0.0.1"),
  Database   = proplists:get_value(database, Params, "openswitch"),
  Username   = proplists:get_value(username, Params),
  Password   = proplists:get_value(password, Params),
  DriverOpts = proplists:get_value(options,  Params, []),

  Driver = get_dbd_module(),

  %% TODO common errors/exceptions?
  Ret = Driver:connect(Host, Database, Username, Password, DriverOpts),
  Ret.

%%--------------------------------------------------------------------------------------------------

disconnect(C) ->
  Driver = get_dbd_module(),
  
  %% TODO common errors/exceptions?
  Ret = Driver:disconnect(C),
  Ret.

%%--------------------------------------------------------------------------------------------------

execute(SQL) when is_list(SQL) ->
  execute(SQL, []).

execute(SQL, Args) when is_list(SQL), is_list(Args) ->
  Driver = get_dbd_module(),

  {ok, C} = connect(),
  Ret = Driver:execute(C, SQL, Args),
  ok = disconnect(C),
  Ret.
  
%%--------------------------------------------------------------------------------------------------

select_proplists(SQL) when is_list(SQL) ->
  select_proplists(SQL, []).

select_proplists(SQL, Args) when is_list(SQL), is_list(Args) ->
  Driver = get_dbd_module(),

  {ok, C} = connect(),
  Ret = Driver:select_proplists(C, SQL, Args),
  ok = disconnect(C),
  Ret.

%%--------------------------------------------------------------------------------------------------

get_dbd_module() ->
  case application:get_env(osw_dbi, driver) of
    {ok, Driver} -> list_to_atom("gen_dbd_" ++ atom_to_list(Driver));
    _            -> throw(undefined_database_driver)
  end.

%%--------------------------------------------------------------------------------------------------