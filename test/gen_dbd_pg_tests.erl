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
  ?assertEqual(C#gen_dbi.driver, gen_dbd_pg),
  ?assertEqual(C#gen_dbi.handle, pid),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_fail_host_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error,{{badmatch,{error,nxdomain}},[]}}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  {error, invalid_hostname} = gen_dbi:connect(pg, "host", "db", "user", "passwd", []),

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

  {error, invalid_password} = gen_dbi:connect(pg, "host", "db", "xxx", "passwd", []),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_fail_password_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error, invalid_password}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  {error, invalid_password} = gen_dbi:connect(pg, "host", "db", "user", "xxx", []),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

connect_fail_database_test() ->
  F1 = fun(_Host, _User, _Passwd, _Opts) ->
    {error,  <<"3D000">>}
  end,

  meck:new(pgsql),
  meck:expect(pgsql, connect, F1),

  {error, invalid_database} = gen_dbi:connect(pg, "host", "xxx", "user", "passwd", []),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------

disconnect_ok_test() ->
  F1 = fun(C) ->
    ?assertEqual(C, pid),
    ok
  end,

  C = #gen_dbi{driver = gen_dbd_pg, handle = pid},

  meck:new(pgsql),
  meck:expect(pgsql, close, F1),

  ok = gen_dbi:disconnect(C),

  meck:unload(pgsql).

%%--------------------------------------------------------------------------------------------------  