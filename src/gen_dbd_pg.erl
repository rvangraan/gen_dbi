%%--------------------------------------------------------------------------------------------------
-module(gen_dbd_pg).
%%--------------------------------------------------------------------------------------------------
-export([
  connect/5,
  disconnect/1,
  trx_begin/1,
  trx_commit/1,
  trx_rollback/1,
  prepare/2,
  execute/2,
  execute/3,
  fetch_proplists/3,
  fetch_lists/3,
  fetch_tuples/3,
  fetch_records/4,
  fetch_structs/4
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
      error_logger:error_msg("unknown gen_dbd_pg:connect/5 error: ~p\n", [Error]),
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
      {ok, loop_fetch_proplists(ColNames, Rows, [])};

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
      {ok, {list_to_tuple(ColNames), [tuple_to_list(R) || R <- Rows]}};

    {error, _Error}=R   -> R;

    _                   -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------

fetch_tuples(C, SQL, Params) ->
  case execute(C, SQL, Params) of
    {ok, Columns, Rows} ->
      ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
      {ok, {list_to_tuple(ColNames), Rows}};

    {error, _Error}=R   -> R;

    _                   -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------

fetch_records(C, SQL, Params, RecordName) ->
  case execute(C, SQL, Params) of
    {ok, _Columns, Rows} ->
      {ok, loop_fetch_records(Rows, RecordName, [])};

    {error, _Error}=R   -> R;

    _                   -> throw(not_a_select)
  end.

loop_fetch_records([], _RecordName, Acc) ->
  lists:reverse(Acc);
loop_fetch_records([Row|Rows], RecordName, Acc) ->
  Row1 = list_to_tuple([RecordName] ++ tuple_to_list(Row)),
  loop_fetch_records(Rows, RecordName, [Row1|Acc]).

%%--------------------------------------------------------------------------------------------------

fetch_structs(C, SQL, Params, StructName) ->
  case execute(C, SQL, Params) of
    {ok, Columns, Rows} ->
      ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
      {ok, loop_fetch_structs(Rows, [], StructName)};

    {error, _Error}=R   -> R;

    _                   -> throw(not_a_select)
  end.

loop_fetch_structs([], Acc, _StructName) ->
  lists:reverse(Acc);

loop_fetch_structs([Row|Rows], Acc, StructName) ->
  Struct = StructName:new_from_tuple(Row),
  loop_fetch_structs(Rows, [Struct|Acc], StructName).

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

    %% TODO: map errors

    {error, Error} ->
      error_logger:error_msg("unknown gen_dbd_pg:execute/3 error: ~p\n", [Error]),
      {error, Error}
  end,

  Ret.

%%--------------------------------------------------------------------------------------------------

prepare(C, SQL) ->
  Handle = C#gen_dbi_dbh.handle,

  Ret = case pgsql:parse(Handle, SQL) of
    {ok, Statement}                       -> {ok, Statement};
    {error,{error,error,<<"42601">>,_,_}} -> {error, syntax_error};

    %% TODO: map errors

    {error, Error} ->
      error_logger:error_msg("unknown gen_dbd_pg:prepare/2 error: ~p\n", [Error]),
      {error, Error}
  end,

  Ret.

%%--------------------------------------------------------------------------------------------------

execute(C, Args) when is_record(C, gen_dbi_sth) ->
  Handle = C#gen_dbi_sth.handle,
  Statement = C#gen_dbi_sth.statement,

  %% TODO: error check?
  ok = pgsql:bind(Handle, Statement, Args),

  % {ok | partial, Rows} = pgsql:execute(C, Statement, [PortalName], [MaxRows]).
  % {ok, Count}          = pgsql:execute(C, Statement, [PortalName]).
  % {ok, Count, Rows}    = pgsql:execute(C, Statement, [PortalName]).
  % All functions return {error, Error} when an error occurs.

  Ret = case pgsql:execute(Handle, Statement) of
    {ok, _RowsOrCount}=R  -> R;
    {partial, _Rows}=R    -> R;
    {ok, _Count, _Rows}=R -> R;

    %% TODO: map errors

    {error, Error} ->
      error_logger:error_msg("unknown gen_dbd_pg:execute/2 error: ~p\n", [Error]),
      {error, Error}
    end,

  Ret.
   
%%--------------------------------------------------------------------------------------------------