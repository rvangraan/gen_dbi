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
  trans/1,
  transaction/1,
  trx/1,
  trans/2,
  transaction/2,
  trx/2,
  connect/0,
  connect/6,
  disconnect/1,
  prepare/2,
  execute/2,
  execute/3,
  fetch_lists/2,
  fetch_lists/3,
  fetch_tuples/2,
  fetch_tuples/3,
  fetch_proplists/2,
  fetch_proplists/3,
  fetch_records/3,
  fetch_records/4,
  fetch_structs/3,
  fetch_structs/4,
  result_to_lists/2,
  result_to_tuples/2,
  result_to_proplists/2,
  result_to_records/3,
  result_to_structs/3
]).
%%--------------------------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------------------------

%% TODO: make errors tuple/3 or record?

%%--------------------------------------------------------------------------------------------------

%% TODO: test if driver app is loaded and if driver can work?
drivers() -> [pg].

%%--------------------------------------------------------------------------------------------------

%% TODO: undefines
connect() ->
  Params = application:get_all_env(gen_dbi),

  Driver     = proplists:get_value(driver,   Params, pg),
  Host       = proplists:get_value(host,     Params, "127.0.0.1"),
  Database   = proplists:get_value(database, Params, "openswitch"),
  Username   = proplists:get_value(username, Params, "postgres"),
  Password   = proplists:get_value(password, Params, "postgres"),
  DriverOpts = proplists:get_value(options,  Params, []),

  connect(Driver, Host, Database, Username, Password, DriverOpts).

%%--------------------------------------------------------------------------------------------------

connect(Driver, Host, Database, Username, Password, DriverOpts) ->
  case is_driver_supported(Driver) of
    true  ->
      Module = get_driver_module(Driver),
      DriverConfig = Module:config(),
      case Module:connect(Host, Database, Username, Password, DriverOpts) of
        {ok, C}        -> 
          DBH = #gen_dbi_dbh{driver = Module, driver_config = DriverConfig, handle = C},
          {ok, DBH};

        {error, Error} -> 
          {error, Error}
      end;

    false -> {error, invalid_driver}
  end.

%%--------------------------------------------------------------------------------------------------

disconnect(C) when is_record(C, gen_dbi_dbh) ->
  Driver = get_driver_module(C),
  ok = Driver:disconnect(C),
  ok.

%%--------------------------------------------------------------------------------------------------
%% Trx
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
trans(C, Fun) when is_record(C, gen_dbi_dbh), is_function(Fun) ->
  trx(C, Fun).

transaction(C, Fun) when is_record(C, gen_dbi_dbh), is_function(Fun) ->
  trx(C, Fun).

trx(C, Fun) when is_record(C, gen_dbi_dbh), is_function(Fun) ->
  try
    ok = trx_begin(C),
    Ret = Fun(C),
    ok = trx_commit(C),
    Ret
  catch
    throw:E ->
      {error, E};

    error:E ->
      error_logger:error_msg(
        "unknown gen_dbi:trx/2 error exception, \nclass: ~p \nexception: ~p\nstacktrace: \n  ~p\n", 
        [error, E, erlang:get_stacktrace()]),
      {error, system_malfunction}     
  after
    ok = trx_rollback(C)
  end.

trans(Fun) when is_function(Fun) ->
  trx(Fun).

transaction(Fun) when is_function(Fun) ->
  trx(Fun).

trx(Fun) when is_function(Fun) ->
  %% TODO: pools
  {ok, C} = connect(),
  Ret = trx(C, Fun),
  ok = disconnect(C),
  Ret.  

%%--------------------------------------------------------------------------------------------------
%% Prepare/Execute
%%--------------------------------------------------------------------------------------------------

%% TODO: on driver side
prepare(C, SQL) when is_record(C, gen_dbi_dbh),  is_list(SQL) ->
  Driver = get_driver_module(C),

  %% TODO: map common errors
  Ret = case Driver:prepare(C, SQL) of
    {ok, Statement} ->
      Sth = #gen_dbi_sth{ driver = C#gen_dbi_dbh.driver, 
                          handle = C#gen_dbi_dbh.handle, 
                          statement = Statement},
      {ok, Sth};

    %% TODO convert all errors to {err, type, details} ??
    {error, Error} -> {error, Error};
    {error, Error, Details} -> {error, Error, Details}
  end,
  Ret.

%%--------------------------------------------------------------------------------------------------

execute(C, Args) when 
  is_record(C, gen_dbi_sth), is_list(Args), C#gen_dbi_sth.statement =/= undefined 
->
  Driver = get_driver_module(C),
  Driver:execute(C, Args);

execute(C, SQL) when is_record(C, gen_dbi_dbh), is_list(SQL) ->
  execute(C, SQL, []).

execute(C, SQL, Args) when is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(C),
  Driver:execute(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------
%% Fetch API: proplists, lists, tuples, records, structs
%%--------------------------------------------------------------------------------------------------

%% TODO: has to work the same with execute and prepare/execute
fetch_proplists(C, SQL) when is_record(C, gen_dbi_dbh), is_list(SQL) ->
  fetch_proplists(C, SQL, []).

fetch_proplists(C, SQL, Args) when is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(C),
  Driver:fetch_proplists(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------

fetch_lists(C, SQL) when is_record(C, gen_dbi_dbh), is_list(SQL) ->
  fetch_lists(C, SQL, []).

fetch_lists(C, SQL, Args) when is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(C),
  Driver:fetch_lists(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------

fetch_tuples(C, SQL) when is_record(C, gen_dbi_dbh), is_list(SQL) ->
  fetch_tuples(C, SQL, []).

fetch_tuples(C, SQL, Args) when is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args) ->
  Driver = get_driver_module(C),
  Driver:fetch_tuples(C, SQL, Args).

%%--------------------------------------------------------------------------------------------------

fetch_records(C, SQL, RecordName) when 
  is_record(C, gen_dbi_dbh), is_list(SQL), is_atom(RecordName) 
->
  fetch_records(C, SQL, [], RecordName).

fetch_records(C, SQL, Args, RecordName) when 
  is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args), is_atom(RecordName) 
->
  Driver = get_driver_module(C),
  Driver:fetch_records(C, SQL, Args, RecordName).

%%--------------------------------------------------------------------------------------------------

fetch_structs(C, SQL, StructName) when 
  is_record(C, gen_dbi_dbh), is_list(SQL), is_atom(StructName) 
->
  fetch_structs(C, SQL, [], StructName).

fetch_structs(C, SQL, Args, StructName) when 
  is_record(C, gen_dbi_dbh), is_list(SQL), is_list(Args), is_atom(StructName) 
->
  Driver = get_driver_module(C),
  Driver:fetch_structs(C, SQL, Args, StructName).

%%--------------------------------------------------------------------------------------------------
%% Convert result to standard structures
%%--------------------------------------------------------------------------------------------------

result_to_lists(C, R) ->
  Driver = get_driver_module(C),
  Driver:result_to_lists(R).

result_to_tuples(C, R) ->
  Driver = get_driver_module(C),
  Driver:result_to_tuples(R).

result_to_proplists(C, R) ->
  Driver = get_driver_module(C),
  Driver:result_to_proplists(R).

result_to_records(C, R, RecordName) ->
  Driver = get_driver_module(C),
  Driver:result_to_records(R, RecordName).

result_to_structs(C, R, StructName) ->
  Driver = get_driver_module(C),
  Driver:result_to_structs(R, StructName).

%%--------------------------------------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------------------------------------

get_driver_module(C) when is_record(C, gen_dbi_sth) ->
  C#gen_dbi_sth.driver;

get_driver_module(C) when is_record(C, gen_dbi_dbh) ->
  C#gen_dbi_dbh.driver;

get_driver_module(Driver) when is_atom(Driver) ->
  list_to_atom("gen_dbd_" ++ atom_to_list(Driver)).

%%--------------------------------------------------------------------------------------------------

is_driver_supported(Driver) ->
  lists:any(fun(E) -> E =:= Driver end, drivers()).

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
%% TODO: C -> DBH
