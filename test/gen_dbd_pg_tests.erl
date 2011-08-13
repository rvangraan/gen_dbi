%%--------------------------------------------------------------------------------------------------
-module(gen_dbd_pg_tests).
%%--------------------------------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").
-include_lib("osw_common/include/testhelper.hrl").
-include_lib("gen_dbi/include/gen_dbi.hrl").
%%--------------------------------------------------------------------------------------------------

-define(CURRENCY_SELECT,
{ok,[{column,<<"u_id">>,int4,4,-1,1},
     {column,<<"u_code">>,bpchar,-1,7,1},
     {column,<<"u_name">>,varchar,-1,259,1}],
    [{840,<<"USD">>,<<"US DOLLAR">>},
     {710,<<"ZAR">>,<<"SOUTH AFRICAN RAND">>},
     {826,<<"GBP">>,<<"POUND STERLING">>},
     {978,<<"EUR">>,<<"EURO">>},
     {392,<<"JPY">>,<<"JAPANESE YEN">>}]}).

%%--------------------------------------------------------------------------------------------------

connect_ok_test() ->
  F1 = fun(Host, User, Passwd, Opts) ->
    ?assertEqual(Host, "host"),
    ?assertEqual(User, "user"),
    ?assertEqual(Passwd, "passwd"),
    ?assertEqual(Opts, [{database,"db"}]),
    {ok, pid}
  end,

  mock(pgsql),
  meck:expect(pgsql, connect, F1),

  {ok, C} = gen_dbi:connect(pg, "host", "db", "user", "passwd", []),
  ?assertEqual(C#gen_dbi_dbh.driver, gen_dbd_pg),
  ?assertEqual(C#gen_dbi_dbh.handle, pid).

%%--------------------------------------------------------------------------------------------------

connect_error_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error, 42}
  end,

  mock(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, 42} , gen_dbi:connect(pg, "host", "db", "user", "passwd", [])).

%%--------------------------------------------------------------------------------------------------

connect_throw_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    throw(42)
  end,

  mock(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error,system_malfunction}, gen_dbi:connect(pg, "host", "db", "user", "passwd", [])).

%%--------------------------------------------------------------------------------------------------

connect_default_params_test() ->
  F1 = fun(Host, Username, Password, DriverOpts) ->
    ?assertEqual(Host,       "127.0.0.1"),
    ?assertEqual(Username,   "postgres"),
    ?assertEqual(Password,   "postgres"),
    ?assertEqual(DriverOpts, [{database, "openswitch"}]),
    {ok, pid}
  end,

  mock(pgsql),
  meck:expect(pgsql, connect, F1),

  {ok, C} = gen_dbi:connect(),
  ?assertEqual(C#gen_dbi_dbh.handle, pid).

%%--------------------------------------------------------------------------------------------------

connect_fail_host_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error,{{badmatch,{error,nxdomain}},[]}}
  end,

  mock(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, invalid_hostname} , gen_dbi:connect(pg, "host", "db", "user", "passwd", [])).

%%--------------------------------------------------------------------------------------------------

connect_fail_driver_test() ->
  {error, invalid_driver} = gen_dbi:connect(mysqlcrap, "host", "db", "user", "passwd", []).

%%--------------------------------------------------------------------------------------------------

connect_fail_username_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error, invalid_password}
  end,

  mock(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, invalid_password} , gen_dbi:connect(pg, "host", "db", "xxx", "passwd", [])).

%%--------------------------------------------------------------------------------------------------

connect_fail_password_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error, invalid_password}
  end,

  mock(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, invalid_password} , gen_dbi:connect(pg, "host", "db", "user", "xxx", [])).

%%--------------------------------------------------------------------------------------------------

connect_fail_database_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error,  <<"3D000">>}
  end,

  mock(pgsql),
  meck:expect(pgsql, connect, F1),

  ?assertEqual({error, invalid_database} , gen_dbi:connect(pg, "host", "xxx", "user", "passwd", [])).

%%--------------------------------------------------------------------------------------------------

disconnect_ok_test() ->
  F1 = fun(C) ->
    ?assertEqual(C, pid),
    ok
  end,

  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  mock(pgsql),
  meck:expect(pgsql, close, F1),

  ?assertEqual(ok , gen_dbi:disconnect(C)).

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

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  ?assertEqual({ok, Columns, Rows} , gen_dbi:execute(C, "SELECT * FROM something", [])).

%%--------------------------------------------------------------------------------------------------

execute_error_test() ->
  F1 = fun(_Handle, _SQL, _Args) ->
    {ok, 42}
  end,

  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  ?assertEqual({ok, 42} , gen_dbi:execute(C, "SELECT * FROM something", [])).

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

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  ?assertEqual({ok, Count} , gen_dbi:execute(C, "UPDATE something SET something = something", [])).

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

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  ?assertEqual(
    {ok, Count, Columns, Rows},
    gen_dbi:execute(C, "INSERT INTO something VALUES (something)", [])
  ).

%%--------------------------------------------------------------------------------------------------

trx_ok_test() ->
  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  F1 = 
    fun(_Handle, "BEGIN", _Args) -> {ok,1,2};
       (_Handle, "COMMIT", _Args) -> {ok,1,2};
       (_Handle, "ROLLBACK", _Args) -> {ok,1,2};
       (_Handle, "SELECT * FROM something", _Args) -> {ok, [], [{},{}]}
  end,

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  Ret = gen_dbi:trx(C, fun(C1) -> gen_dbi:execute(C1, "SELECT * FROM something") end),
  ?assertEqual(Ret, {ok,[],[{},{}]} ).

%%--------------------------------------------------------------------------------------------------

trx_error_test() ->
  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  F1 = 
    fun(_Handle, "BEGIN", _Args) -> {ok,1,2};
       (_Handle, "COMMIT", _Args) -> {ok,1,2};
       (_Handle, "ROLLBACK", _Args) -> {ok,1,2};
       (_Handle, "SELECT * FROM something", _Args) -> throw(43)
  end,

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  Ret = gen_dbi:trx(C, fun(C1) -> gen_dbi:execute(C1, "SELECT * FROM something",[]) end),
  ?assertEqual(Ret, {error, 43}).

%%--------------------------------------------------------------------------------------------------

trx_exception_test() ->
  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},

  F1 = 
    fun(_Handle, "BEGIN", _Args) -> {ok,1,2};
       (_Handle, "COMMIT", _Args) -> {ok,1,2};
       (_Handle, "ROLLBACK", _Args) -> {ok,1,2}
  end,

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  Ret = gen_dbi:trx(C, fun(_C1) -> erlang:error(43) end),
  ?assertEqual(Ret, {error, system_malfunction}).

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

  mock(pgsql),
  meck:expect(pgsql, equery, F1),
  meck:expect(pgsql, connect, F2),
  meck:expect(pgsql, close, F3),

  Ret = gen_dbi:trx(fun(C) -> gen_dbi:execute(C, "SELECT * FROM something",[]) end),
  ?assertEqual(Ret, {ok,[],[{},{}]} ).

%%--------------------------------------------------------------------------------------------------

prepare_ok_test() ->
  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},
  F1 = fun(_C, "SELECT * FROM something WHERE bla = ?") -> {ok, 42} end,

  mock(pgsql),
  meck:expect(pgsql, parse, F1),

  {ok, Ret} = gen_dbi:prepare(C, "SELECT * FROM something WHERE bla = ?"),
  ?assertEqual(Ret#gen_dbi_sth.statement, 42 ).

%%--------------------------------------------------------------------------------------------------

prepare_error_test() ->
  C = #gen_dbi_dbh{driver = gen_dbd_pg, handle = pid},
  F1 = fun(_C, "SLECT * FROM something WHERE bla = ?") -> 
    {error,{error,error,<<"42601">>,[],[]}}
  end,

  F2 = fun(_C, "SLECT * FROM something WHERE bla = ?") -> 
    {error, 42}
  end,

  mock(pgsql),
  meck:expect(pgsql, parse, F1),

  Ret1 = gen_dbi:prepare(C, "SLECT * FROM something WHERE bla = ?"),
  ?assertEqual(Ret1, {error, invalid_syntax, [{message,[]}]} ),

  meck:expect(pgsql, parse, F2),
  Ret2 = gen_dbi:prepare(C, "SLECT * FROM something WHERE bla = ?"),
  ?assertEqual(Ret2, {error, 42} ).

%%--------------------------------------------------------------------------------------------------

prepare_execute_ok_test() ->
  C = #gen_dbi_sth{ driver = gen_dbd_pg, handle = pid, statement = 42},

  F1 = fun(_Handle, _Statement, _Args) -> ok end,
  F2 = fun(_Handle, _Statement) -> {ok, [{},{}]} end,

  mock(pgsql),
  meck:expect(pgsql, bind, F1),
  meck:expect(pgsql, execute, F2),

  Ret = gen_dbi:execute(C, [42]),
  ?assertEqual(Ret, {ok, [{},{}]} ).
  
%%--------------------------------------------------------------------------------------------------

prepare_execute_error_test() ->
  C = #gen_dbi_sth{ driver = gen_dbd_pg, handle = pid, statement = 42},

  F1 = fun(_Handle, _Statement, _Args) -> ok end,
  F2 = fun(_Handle, _Statement) -> {error, 42} end,
  F3 = fun(_Handle) -> ok end,

  mock(pgsql),
  meck:expect(pgsql, bind, F1),
  meck:expect(pgsql, execute, F2),
  meck:expect(pgsql, sync, F3),

  Ret = gen_dbi:execute(C, [42]),
  ?assertEqual(Ret, {error, 42} ).
  
%%--------------------------------------------------------------------------------------------------

fetch_lists_ok_test() ->
  C = #gen_dbi_dbh{ driver = gen_dbd_pg, handle = pid},

  F1 = fun(_Handle, _SQL, _Args) -> ?CURRENCY_SELECT end,

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  {ok, Ret} = gen_dbi:fetch_lists(C, "SELECT * FROM CURRENCY"),
  ?assertEqual(Ret,
    {
      {u_id,u_code,u_name},
      [
        [840,<<"USD">>,<<"US DOLLAR">>],
        [710,<<"ZAR">>,<<"SOUTH AFRICAN RAND">>],
        [826,<<"GBP">>,<<"POUND STERLING">>],
        [978,<<"EUR">>,<<"EURO">>],
        [392,<<"JPY">>,<<"JAPANESE YEN">>]
      ]
    }).

%%--------------------------------------------------------------------------------------------------

fetch_proplists_ok_test() ->
  C = #gen_dbi_dbh{ driver = gen_dbd_pg, handle = pid},

  F1 = fun(_Handle, _SQL, _Args) -> ?CURRENCY_SELECT end,

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  {ok, Ret} = gen_dbi:fetch_proplists(C, "SELECT * FROM CURRENCY"),
  ?assertEqual(Ret,
    [
     [{u_id,840},
      {u_code,<<"USD">>},
      {u_name,<<"US DOLLAR">>}],
     [{u_id,710},
      {u_code,<<"ZAR">>},
      {u_name,<<"SOUTH AFRICAN RAND">>}],
     [{u_id,826},
      {u_code,<<"GBP">>},
      {u_name,<<"POUND STERLING">>}],
     [{u_id,978},
      {u_code,<<"EUR">>},
      {u_name,<<"EURO">>}],
     [{u_id,392},
      {u_code,<<"JPY">>},
      {u_name,<<"JAPANESE YEN">>}]
    ]).

%%--------------------------------------------------------------------------------------------------

fetch_tuples_ok_test() ->
  C = #gen_dbi_dbh{ driver = gen_dbd_pg, handle = pid},

  F1 = fun(_Handle, _SQL, _Args) -> ?CURRENCY_SELECT end,

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  {ok, Ret} = gen_dbi:fetch_tuples(C, "SELECT * FROM CURRENCY"),
  ?assertEqual(Ret,
    {{u_id,u_code,u_name},
     [{840,<<"USD">>,<<"US DOLLAR">>},
      {710,<<"ZAR">>,<<"SOUTH AFRICAN RAND">>},
      {826,<<"GBP">>,<<"POUND STERLING">>},
      {978,<<"EUR">>,<<"EURO">>},
      {392,<<"JPY">>,<<"JAPANESE YEN">>}]
    }).

%%--------------------------------------------------------------------------------------------------

fetch_records_ok_test() ->
  C = #gen_dbi_dbh{ driver = gen_dbd_pg, handle = pid},

  F1 = fun(_Handle, _SQL, _Args) -> ?CURRENCY_SELECT end,

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  {ok, Ret} = gen_dbi:fetch_records(C, "SELECT * FROM CURRENCY", some_record),
  ?assertEqual(Ret,
    [
      {some_record,840,<<"USD">>,<<"US DOLLAR">>},
      {some_record,710,<<"ZAR">>,<<"SOUTH AFRICAN RAND">>},
      {some_record,826,<<"GBP">>,<<"POUND STERLING">>},
      {some_record,978,<<"EUR">>,<<"EURO">>},
      {some_record,392,<<"JPY">>,<<"JAPANESE YEN">>}
    ]).

%%--------------------------------------------------------------------------------------------------

fetch_structs_ok_test() ->
  C = #gen_dbi_dbh{ driver = gen_dbd_pg, handle = pid},

  F1 = fun(_Handle, _SQL, _Args) -> ?CURRENCY_SELECT end,

  mock(pgsql),
  meck:expect(pgsql, equery, F1),

  {ok, Ret} = gen_dbi:fetch_structs(C, "SELECT * FROM CURRENCY", db_currency),
  ?assertEqual(Ret,
    [
      {db_currency,840,<<"USD">>,<<"US DOLLAR">>},
      {db_currency,710,<<"ZAR">>,<<"SOUTH AFRICAN RAND">>},
      {db_currency,826,<<"GBP">>,<<"POUND STERLING">>},
      {db_currency,978,<<"EUR">>,<<"EURO">>},
      {db_currency,392,<<"JPY">>,<<"JAPANESE YEN">>}
    ]).

%%--------------------------------------------------------------------------------------------------

start_stop_test() ->
  ok = application:start(gen_dbi),
  ok = application:stop(gen_dbi),
  ok = application:start(gen_dbi),
  ok.

%%--------------------------------------------------------------------------------------------------
