%%--------------------------------------------------------------------------------------------------
-module(gen_dbd_pg).
%%--------------------------------------------------------------------------------------------------
-export([
  connect/5,
  disconnect/1,
  trx_begin/1,
  trx_commit/1,
  trx_rollback/1,
  execute/2,
  execute/3,
  fetch_proplists/3,
  fetch_lists/3,
  fetch_tuples/3,
  fetch_records/4
]).
%%--------------------------------------------------------------------------------------------------
-include_lib("gen_dbi/include/gen_dbi.hrl").
%%--------------------------------------------------------------------------------------------------

%% TODO: merge/validate/convert? opts
connect(Host, Database, Username, Password, _DBDOpts) ->
  try
   case pgsql:connect(Host, Username, Password, [{database, Database}]) of
    {ok, Pid}                                -> {ok, Pid};
    {error, {{badmatch,{error,nxdomain}},_}} -> {error, invalid_hostname};
    {error, invalid_password}                -> {error, invalid_password};
    {error, <<"3D000">>}                     -> {error, invalid_database};
    
    {error, Error} -> 
      error_logger:error_msg("unknown gen_dbd_pg:connect error: ~p\n", [Error]),
      {error, Error}
  end
  catch
    C:E ->
      error_logger:error_msg(
        "unknown gen_dbd_pg:connect exception \nclass: ~p \nexception: ~p\n",[C,E]),
        {error, unable_to_connect}
  end.

%%--------------------------------------------------------------------------------------------------

disconnect(C) ->
  ok = pgsql:close(C#gen_dbi_dbh.handle),
  ok.

%%--------------------------------------------------------------------------------------------------

fetch_proplists(C, SQL, Params) ->
  case execute(C, SQL, Params) of
    {ok, Columns, Rows} ->
      ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
      loop_fetch_proplists(ColNames, Rows, []);

    {error, _Error}=R   -> R;

    _                   -> throw(not_a_select)
  end.
      
loop_fetch_proplists(_Columns, [], Acc) ->
  lists:reverse(Acc);

loop_fetch_proplists(Columns, [Row|Rows], Acc) ->
  NewRow = lists:zipwith(fun(X,Y) -> {X,Y} end, Columns, tuple_to_list(Row)),
  loop_fetch_proplists(Columns, Rows, [NewRow|Acc]).
  
%%--------------------------------------------------------------------------------------------------

fetch_lists(C, SQL, Params) ->
  case execute(C, SQL, Params) of
    {ok, Columns, Rows} ->
      ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
      {list_to_tuple(ColNames), [tuple_to_list(R) || R <- Rows]};

    {error, _Error}=R   -> R;

    _                   -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------

fetch_tuples(C, SQL, Params) ->
  case execute(C, SQL, Params) of
    {ok, Columns, Rows} ->
      ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
      {list_to_tuple(ColNames), Rows};

    {error, _Error}=R   -> R;

    _                   -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------

fetch_records(_C, _SQL, _Params, _RecordName) ->
  todo.

%%--------------------------------------------------------------------------------------------------

trx_begin(C) ->
  {ok,_,_} = execute(C, "BEGIN", []),
  ok.

trx_rollback(C) ->
  {ok,_,_} = execute(C, "ROLLBACK",[]),
  ok.

trx_commit(C) ->
  {ok,_,_} = execute(C, "COMMIT", []),
  ok.

%%--------------------------------------------------------------------------------------------------

execute(C, SQL, Args) when is_record(C, gen_dbi_dbh) ->

  % {ok, Columns, Rows}        = pgsql:equery(C, "select ...", [Parameters]).
  % {ok, Count}                = pgsql:equery(C, "update ...", [Parameters]).
  % {ok, Count, Columns, Rows} = pgsql:equery(C, "insert ... returning ...", [Parameters]).
  % {error, Error}             = pgsql:equery(C, "invalid SQL", [Parameters]).           

  Ret = case pgsql:equery(C#gen_dbi_dbh.handle, SQL, Args) of
    {ok, _Columns, _Rows}=R         -> R;
    {ok, _Count}=R                  -> R;
    {ok, _Count, _Columns, _Rows}=R -> R;
    {error, _Error}=R               -> R
  end,

  Ret.

%%--------------------------------------------------------------------------------------------------

execute(C, Args) when is_record(C, gen_dbi_sth) ->
  Handle = C#gen_dbi_sth.handle,
  Statement = C#gen_dbi_sth.statement,
  ok = pgsql:bind(Handle, Statement, Args),

  % {ok | partial, Rows} = pgsql:execute(C, Statement, [PortalName], [MaxRows]).
  % {ok, Count}          = pgsql:execute(C, Statement, [PortalName]).
  % {ok, Count, Rows}    = pgsql:execute(C, Statement, [PortalName]).
  % All functions return {error, Error} when an error occurs.

  Ret = case pgsql:execute(Handle, Statement) of
    {ok, _RowsOrCount}=R  -> R;
    {partial, _Rows}=R    -> R;
    {ok, _Count, _Rows}=R -> R;
    {error, _Error}=R     -> R
    end,

  Ret.
   
%%--------------------------------------------------------------------------------------------------