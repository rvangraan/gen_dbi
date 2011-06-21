%%--------------------------------------------------------------------------------------------------
-module(gen_dbi).
%%--------------------------------------------------------------------------------------------------
-include_lib("gen_dbi/include/gen_dbi.hrl").
%%--------------------------------------------------------------------------------------------------
-export([
  drivers/0,
  trx_begin/1,
  trx_commit/1,
  trx_rollback/1,
  trx/1,
  trx/2,
  connect/0,
  connect/6,
  disconnect/1,
  prepare/2,
  execute/2,
  execute/3,
  fetch_proplists/2,
  fetch_proplists/3,
  fetch_lists/2,
  fetch_lists/3,
  fetch_tuples/2,
  fetch_tuples/3,
  test/0
]).
%%--------------------------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------------------------

drivers() -> [pg].

%%--------------------------------------------------------------------------------------------------

%% TODO: undefines
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
  %% TODO common errors/exceptions?
  case is_driver_supported(Driver) of
    true  ->
      Module = get_driver_module(Driver), 
      case Module:connect(Host, Database, Username, Password, DriverOpts) of
        {ok, C}        -> {ok, #gen_dbi_dbh{driver = Module, handle = C}};
        {error, Error} -> {error, Error}
      end;

    false -> {error, invalid_driver}
  end.

%%--------------------------------------------------------------------------------------------------

disconnect(C) when is_record(C, gen_dbi_dbh) ->
  Driver = get_driver_module(C),
  
  %% TODO common errors/exceptions?
  ok = Driver:disconnect(C),
  ok.

%%--------------------------------------------------------------------------------------------------

trx_begin(C) ->
  Driver = get_driver_module(C),
  Driver:trx_begin(C).

trx_commit(C) ->
  Driver = get_driver_module(C),
  Driver:trx_commit(C).

trx_rollback(C) ->
  Driver = get_driver_module(C),
  Driver:trx_rollback(C).

%%--------------------------------------------------------------------------------------------------

trx(C, Fun) when is_record(C, gen_dbi_dbh), is_function(Fun) ->
  try
    ok = trx_begin(C),
    Ret = Fun(C),
    ok = trx_commit(C),
    Ret
  catch
    throw:E ->
      {error, E};
    C:E ->
      error_logger:error_message(
        "unknown gen_dbi:trx exception, \nclass: ~p, \nexception: ~p\n", [C,E]),
      {error, system_malfunction}       
  after
    ok = trx_rollback(C)
  end.

trx(Fun) when is_function(Fun) ->
  %% TODO: pools
  {ok, C} = connect(),
  Ret = trx(C, Fun),
  ok = disconnect(C),
  Ret.  

%%--------------------------------------------------------------------------------------------------

prepare(C, SQL) when is_record(C, gen_dbi_dbh),  is_list(SQL) ->
  Driver = get_driver_module(C),

  %% TODO: errors
  {ok, Statement} = Driver:prepare(C, SQL),
  Sth = #gen_dbi_sth{ driver = #gen_dbi_dbh.driver, 
                      handle = #gen_dbi_dbh.handle, 
                      statement = Statement},
  {ok, Sth}.

%%--------------------------------------------------------------------------------------------------

execute(C, Args) when is_record(C, gen_dbi_sth), is_list(Args) ->
  Driver = get_driver_module(C),
  Driver:execute(C, Args);

execute(C, SQL) when is_record(C, gen_dbi_dbh), is_list(SQL) ->
  execute(C, SQL, []).

execute(C, SQL, Args) when is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(C),
  Driver:execute(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------

%% TODO: has to work the same with execute and prepare/execute
fetch_proplists(C, SQL) when is_record(C, gen_dbi_dbh), is_list(SQL) ->
  fetch_proplists(C, SQL, []).

fetch_proplists(C, SQL, Args) when is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(),
  Driver:fetch_proplists(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------

fetch_lists(C, SQL) when is_record(C, gen_dbi_dbh), is_list(SQL) ->
  fetch_lists(C, SQL, []).

fetch_lists(C, SQL, Args) when is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(),
  Driver:fetch_lists(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------

fetch_tuples(C, SQL) when is_record(C, gen_dbi_dbh), is_list(SQL) ->
  fetch_tuples(C, SQL, []).

fetch_tuples(C, SQL, Args) when is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(),
  Driver:fetch_tuples(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------------------------------------

get_driver_module(C) when is_record(C, gen_dbi_sth) ->
  C#gen_dbi_sth.driver;

get_driver_module(C) when is_record(C, gen_dbi_dbh) ->
  C#gen_dbi_dbh.driver;

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

test() ->
  F = fun(C) ->
    gen_dbi:execute(C, "INSERT INTO currency (1,'BLA', 'BLABLABLA')"),
    throw(lalala)
  end,

  gen_dbi:trx(F),
  ok.

%%--------------------------------------------------------------------------------------------------

%% TODO: select_*
%% TODO: prepare, bind, execute
%% TODO: errors
%% DONE: begin/commit/roll_back
%% TODO: integrate with TM
%% TODO: raiseError
%% TODO: datetime/conversion?
%% TODO: amount conversion
%% TODO: default timeouts
%% TODO: test statements