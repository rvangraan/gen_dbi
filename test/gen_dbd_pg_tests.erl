%%--------------------------------------------------------------------------------------------------
-module(gen_dbd_pg_tests).
%%--------------------------------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").
-include_lib("gen_dbi/include/gen_dbi.hrl").
%%--------------------------------------------------------------------------------------------------

connect_ok_test() ->
  F1 = fun(Host, User, Passwd, Opts) ->
    ?assertEqual(Host, "host"),
    ?assertEqual(User, "user"),
    ?assertEqual(Passwd, "passwd"),
    ?assertEqual(Opts, [{database,"db"}]),
    {ok, pid}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  {ok, C} = gen_dbi:connect(pg, "host", "db", "user", "passwd", []),
  ?assertEqual(C#gen_dbi_dbh.driver, gen_dbd_pg),
  ?assertEqual(C#gen_dbi_dbh.handle, pid),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_error_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error, 42}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, 42} , gen_dbi:connect(pg, "host", "db", "user", "passwd", [])),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_throw_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    throw(42)
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, unable_to_connect} , gen_dbi:connect(pg, "host", "db", "user", "passwd", [])),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_default_params_test() ->
  F1 = fun(Host, Username, Password, DriverOpts) ->
    ?assertEqual(Host,       "127.0.0.1"),
    ?assertEqual(Username,   "postgres"),
    ?assertEqual(Password,   "postgres"),
    ?assertEqual(DriverOpts, [{database, "openswitch"}]),
    {ok, pid}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  {ok, C} = gen_dbi:connect(),
  ?assertEqual(C#gen_dbi_dbh.handle, pid),

  meck:unload(pgsql).  

%%--------------------------------------------------------------------------------------------------

connect_fail_host_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error,{{badmatch,{error,nxdomain}},[]}}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, invalid_hostname} , gen_dbi:connect(pg, "host", "db", "user", "passwd", [])),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_fail_driver_test() ->
  {error, invalid_driver} = gen_dbi:connect(mysqlcrap, "host", "db", "user", "passwd", []).

%%--------------------------------------------------------------------------------------------------

connect_fail_username_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error, invalid_password}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, invalid_password} , gen_dbi:connect(pg, "host", "db", "xxx", "passwd", [])),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_fail_password_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error, invalid_password}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, invalid_password} , gen_dbi:connect(pg, "host", "db", "user", "xxx", [])),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_fail_database_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error,  <<"3D000">>}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, invalid_database} , gen_dbi:connect(pg, "host", "xxx", "user", "passwd", [])),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

disconnect_ok_test() ->
  F1 = fun(C) ->
    ?assertEqual(C, pid),
    ok
  end,

  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  meck:new(pgsql),
  meck:expect(pgsql, close, F1),

  ?assertEqual(ok , gen_dbi:disconnect(C)),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

execute_select_test() ->
  Columns = [],
  Rows = [{},{}],

  F1 = fun(Handle, SQL, Args) ->
    ?assertEqual(Handle, pid),
    ?assertEqual(SQL, "SELECT * FROM something"),
    ?assertEqual(Args, []),
    {ok, Columns, Rows}
  end,

  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  meck:new(pgsql),
  meck:expect(pgsql, equery, F1),

  ?assertEqual({ok, Columns, Rows} , gen_dbi:execute(C, "SELECT * FROM something", [])),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

execute_update_test() ->
  Count = 42,

  F1 = fun(Handle, SQL, Args) ->
    ?assertEqual(Handle, pid),
    ?assertEqual(SQL, "UPDATE something SET something = something"),
    ?assertEqual(Args, []),
    {ok, Count}
  end,

  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  meck:new(pgsql),
  meck:expect(pgsql, equery, F1),

  ?assertEqual({ok, Count} , gen_dbi:execute(C, "UPDATE something SET something = something", [])),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

execute_insert_test() ->
  Columns = [],
  Rows = [{},{}],
  Count = 42,

  F1 = fun(Handle, SQL, Args) ->
    ?assertEqual(Handle, pid),
    ?assertEqual(SQL, "INSERT INTO something VALUES (something)"),
    ?assertEqual(Args, []),
    {ok, Count, Columns, Rows}
  end,

  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  meck:new(pgsql),
  meck:expect(pgsql, equery, F1),

  ?assertEqual(
    {ok, Count, Columns, Rows},
    gen_dbi:execute(C, "INSERT INTO something VALUES (something)", [])
  ),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

trx_ok_test() ->
  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  F1 = 
    fun(_Handle, "BEGIN", _Args) -> {ok,1,2};
       (_Handle, "COMMIT", _Args) -> {ok,1,2};
       (_Handle, "ROLLBACK", _Args) -> {ok,1,2};
       (_Handle, "SELECT * FROM something", _Args) -> {ok, [], [{},{}]}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, equery, F1),

  Ret = gen_dbi:trx(C, fun(C1) -> gen_dbi:execute(C1, "SELECT * FROM something",[]) end),
  ?assertEqual(Ret, {ok,[],[{},{}]} ),
  
  meck:unload(pgsql).


%%--------------------------------------------------------------------------------------------------

trx_error_test() ->
  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  F1 = 
    fun(_Handle, "BEGIN", _Args) -> {ok,1,2};
       (_Handle, "COMMIT", _Args) -> {ok,1,2};
       (_Handle, "ROLLBACK", _Args) -> {ok,1,2};
       (_Handle, "SELECT * FROM something", _Args) -> throw(43)
  end,

  meck:new(pgsql),
  meck:expect(pgsql, equery, F1),

  Ret = gen_dbi:trx(C, fun(C1) -> gen_dbi:execute(C1, "SELECT * FROM something",[]) end),
  ?assertEqual(Ret, {error, 43}),
  
  meck:unload(pgsql).  

%%--------------------------------------------------------------------------------------------------

trx_exception_test() ->
  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  F1 = 
    fun(_Handle, "BEGIN", _Args) -> {ok,1,2};
       (_Handle, "COMMIT", _Args) -> {ok,1,2};
       (_Handle, "ROLLBACK", _Args) -> {ok,1,2}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, equery, F1),

  Ret = gen_dbi:trx(C, fun(C1) -> erlang:error(43) end),
  ?assertEqual(Ret, {error, system_malfunction}),
  
  meck:unload(pgsql).  

%%--------------------------------------------------------------------------------------------------

trx_ok_auto_connect_test() ->
  F1 = 
    fun(_Handle, "BEGIN", _Args) -> {ok,1,2};
       (_Handle, "COMMIT", _Args) -> {ok,1,2};
       (_Handle, "ROLLBACK", _Args) -> {ok,1,2};
       (_Handle, "SELECT * FROM something", _Args) -> {ok, [], [{},{}]}
  end,

  F2 = fun(_Host, _User, _Passwd, _Opts) ->
    {ok, pid}
  end,

  F3 = fun(_Pid) ->
    ok
  end,

  meck:new(pgsql),
  meck:expect(pgsql, equery, F1),
  meck:expect(pgsql, connect, F2),
  meck:expect(pgsql, close, F3),

  Ret = gen_dbi:trx(fun(C) -> gen_dbi:execute(C, "SELECT * FROM something",[]) end),
  ?assertEqual(Ret, {ok,[],[{},{}]} ),

  meck:unload(pgsql).  
  
%%--------------------------------------------------------------------------------------------------


start_stop_test() ->
  ok = application:start(gen_dbi),
  ok = application:stop(gen_dbi),
  ok = application:start(gen_dbi),
  ok.

%%--------------------------------------------------------------------------------------------------
