%%--------------------------------------------------------------------------------------------------
-module(gen_dbi).
%%--------------------------------------------------------------------------------------------------
-include_lib("gen_dbi/include/gen_dbi.hrl").
%%--------------------------------------------------------------------------------------------------
-export([
  drivers/0,
  trans/1,
  trans/2,
  connect/0,
  connect/6,
  disconnect/1,
  execute/1,
  execute/2,
  execute/3,
  fetch_proplists/2,
  fetch_proplists/3,
  fetch_lists/2,
  fetch_lists/3,
  fetch_tuples/2,
  fetch_tuples/3
]).
%%--------------------------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------------------------

drivers() -> [pg].

%%--------------------------------------------------------------------------------------------------

connect() ->
  Params = application:get_all_env(gen_dbi),

  Driver     = proplists:get_value(driver,   Params, pg),
  Host       = proplists:get_value(host,     Params, "127.0.0.1"),
  Database   = proplists:get_value(database, Params, "openswitch"),
  Username   = proplists:get_value(username, Params),
  Password   = proplists:get_value(password, Params),
  DriverOpts = proplists:get_value(options,  Params, []),

  connect(Driver, Host, Database, Username, Password, DriverOpts).

%%--------------------------------------------------------------------------------------------------

connect(Driver, Host, Database, Username, Password, DriverOpts) ->
  Module = get_driver_module(Driver),

  %% TODO common errors/exceptions?
  case is_driver_supported(Driver) of
    true  -> 
        case Module:connect(Host, Database, Username, Password, DriverOpts) of
          {ok, C}        -> {ok, #gen_dbi{driver = Module, handle = C}};
          {error, Error} -> {error, Error}
        end;

    false -> {error, invalid_driver}
  end.

%%--------------------------------------------------------------------------------------------------

disconnect(C) when is_record(C, gen_dbi) ->
  Driver = get_driver_module(C),
  
  %% TODO common errors/exceptions?
  ok = Driver:disconnect(C),
  ok.

%%--------------------------------------------------------------------------------------------------

trans(C, Fun) when is_record(C, gen_dbi), is_function(Fun) ->
  Fun(C).

trans(Fun) when is_function(Fun) ->
  {ok, C} = connect(),
  Ret = Fun(C),
  ok = disconnect(C),
  Ret.  

%%--------------------------------------------------------------------------------------------------

execute(SQL) when is_list(SQL) -> 
  execute(SQL, []).
  
execute(SQL, Args) when is_list(SQL), is_list(Args) ->
  trans(fun(C) -> execute(C, SQL, []) end);

%%--------------------------------------------------------------------------------------------------

execute(C, SQL) when is_record(C, gen_dbi), is_list(SQL) ->
  execute(C, SQL, []).

execute(C, SQL, Args) when is_record(C, gen_dbi), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(C),
  Driver:execute(C, SQL, Args).
  
%%--------------------------------------------------------------------------------------------------

fetch_proplists(C, SQL) when is_record(C, gen_dbi), is_list(SQL) ->
  fetch_proplists(C, SQL, []).

fetch_proplists(C, SQL, Args) when is_record(C, gen_dbi), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(),
  Driver:fetch_proplists(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------

fetch_lists(C, SQL) when is_record(C, gen_dbi), is_list(SQL) ->
  fetch_lists(C, SQL, []).

fetch_lists(C, SQL, Args) when is_record(C, gen_dbi), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(),
  Driver:fetch_lists(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------

fetch_tuples(C, SQL) when is_record(C, gen_dbi), is_list(SQL) ->
  fetch_tuples(C, SQL, []).

fetch_tuples(C, SQL, Args) when is_record(C, gen_dbi), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(),
  Driver:fetch_tuples(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------------------------------------

get_driver_module(C) when is_record(C, gen_dbi) ->
  C#gen_dbi.driver;

get_driver_module(Driver) when is_atom(Driver) ->
  list_to_atom("gen_dbd_" ++ atom_to_list(Driver)).

get_driver_module() ->
  case application:get_env(gen_dbi, driver) of
    {ok, Driver} -> list_to_atom("gen_dbd_" ++ atom_to_list(Driver));
    _            -> throw(undefined_database_driver)
  end.

%%--------------------------------------------------------------------------------------------------

is_driver_supported(Driver) ->
  lists:any(fun(E) -> E =:= Driver end, drivers()).

%%--------------------------------------------------------------------------------------------------