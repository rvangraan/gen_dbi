%%--------------------------------------------------------------------------------------------------
-module(gen_dbd_pg).
%%--------------------------------------------------------------------------------------------------
-export([
  config/0,
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
  fetch_structs/4,
  result_to_proplists/1,
  result_to_tuples/1,
  result_to_lists/1,
  result_to_records/2,
  result_to_structs/2
]).
%%--------------------------------------------------------------------------------------------------
-include_lib("gen_dbi/include/gen_dbi.hrl").
%%--------------------------------------------------------------------------------------------------

%%--------------------------------------------------------------------------------------------------
%% DBD API
%%--------------------------------------------------------------------------------------------------

config() -> [{placeholder, "$="}, {returning, true}].

%%--------------------------------------------------------------------------------------------------

%% TODO: merge/validate/convert? opts
connect(Host, Database, Username, Password, _DBDOpts) ->
  try
   case pgsql:connect(Host, Username, Password, [{database, Database}]) of
    {ok, Pid}      -> {ok, Pid};
    {error, Error} -> handle_error(Error)
  end
  catch
    C:E ->
      handle_exception(C, E)
  end.

%%--------------------------------------------------------------------------------------------------

disconnect(C) ->
  ok = pgsql:close(C#gen_dbi_dbh.handle),
  ok.

%%--------------------------------------------------------------------------------------------------

fetch_proplists(C, SQL, Params) ->
  case execute(C, SQL, Params) of
    {ok, _Columns, _Rows}=R -> {ok, result_to_proplists(R)};
    {error, Error}          -> handle_error(Error);
    _                       -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------

fetch_lists(C, SQL, Params) ->
  case execute(C, SQL, Params) of
    {ok, _Columns, _Rows}=R -> {ok, result_to_lists(R)};
    {error, Error}          -> handle_error(Error);
    _                       -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------

fetch_tuples(C, SQL, Params) ->
  case execute(C, SQL, Params) of
    {ok, _Columns, _Rows}=R -> {ok, result_to_tuples(R)};
    {error, Error}          -> handle_error(Error);
    _                       -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------

fetch_records(C, SQL, Params, RecordName) ->
  case execute(C, SQL, Params) of
    {ok, _Columns, _Rows}=R -> {ok, result_to_records(R, RecordName)};
    {error, Error}        -> handle_error(Error);
    _                     -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------  

%% TODO tuple/3 errors, fuck epgsql!!!
fetch_structs(C, SQL, Params, StructName) ->
  case execute(C, SQL, Params) of
    {ok, _Columns, _Rows}=R -> {ok, result_to_structs(R, StructName)};
    {error, Error}        -> handle_error(Error);
    _                     -> throw(not_a_select)
  end.

%%--------------------------------------------------------------------------------------------------

result_to_proplists(Result) ->
  {Columns, Rows} = get_cols_and_rows(Result),
  ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
  loop_result_to_proplists(ColNames, Rows, []).
      
loop_result_to_proplists(_Columns, [], Acc) ->
  lists:reverse(Acc);

loop_result_to_proplists(Columns, [Row|Rows], Acc) ->
  NewRow = lists:zipwith(fun(X,Y) -> {X,Y} end, Columns, tuple_to_list(Row)),
  loop_result_to_proplists(Columns, Rows, [NewRow|Acc]).

%%--------------------------------------------------------------------------------------------------

result_to_lists(Result) ->
  {Columns, Rows} = get_cols_and_rows(Result),
  ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
  {list_to_tuple(ColNames), [tuple_to_list(R) || R <- Rows]}.

%%--------------------------------------------------------------------------------------------------

result_to_tuples(Result) ->
  {Columns, Rows} = get_cols_and_rows(Result),
  ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
  {list_to_tuple(ColNames), Rows}.

%%--------------------------------------------------------------------------------------------------

result_to_records(Result, RecordName) ->
  {_Columns, Rows} = get_cols_and_rows(Result),
  loop_result_to_records(Rows, RecordName, []).

loop_result_to_records([], _RecordName, Acc) ->
  lists:reverse(Acc);
loop_result_to_records([Row|Rows], RecordName, Acc) ->
  Row1 = list_to_tuple([RecordName] ++ tuple_to_list(Row)),
  loop_result_to_records(Rows, RecordName, [Row1|Acc]).

%%--------------------------------------------------------------------------------------------------

result_to_structs(Result, StructName) ->
  {_Columns, Rows} = get_cols_and_rows(Result),
  loop_result_to_structs(Rows, [], StructName).

loop_result_to_structs([], Acc, _StructName) ->
  lists:reverse(Acc);

loop_result_to_structs([Row|Rows], Acc, StructName) ->
  Struct = StructName:new_from_tuple(Row),
  loop_result_to_structs(Rows, [Struct|Acc], StructName).

%%--------------------------------------------------------------------------------------------------

trx_begin(C) ->
  case execute(C, "BEGIN",[]) of
    {ok,_,_} -> ok;
    ok -> ok
  end,
  ok.

trx_rollback(C) ->
  case execute(C, "ROLLBACK",[]) of
    {ok,_,_} -> ok;
    ok -> ok
  end,
  ok.

trx_commit(C) ->
  case execute(C, "COMMIT", []) of
    {ok,_,_} -> ok;
    ok -> ok
  end,
  ok.

%%--------------------------------------------------------------------------------------------------

execute(C, SQL, Args) when is_record(C, gen_dbi_dbh) ->

  % {ok, Columns, Rows}        = pgsql:equery(C, "select ...", [Parameters]).
  % {ok, Count}                = pgsql:equery(C, "update ...", [Parameters]).
  % {ok, Count, Columns, Rows} = pgsql:equery(C, "insert ... returning ...", [Parameters]).
  % {error, Error}             = pgsql:equery(C, "invalid SQL", [Parameters]).           

  Handle = C#gen_dbi_dbh.handle,

  Ret = case pgsql:equery(Handle, SQL, Args) of
    {ok, _Columns, _Rows}=R         -> R;
    {ok, _Count}=R                  -> R;
    {ok, _Count, _Columns, _Rows}=R -> R;
    {error, Error} ->    
      ok = pgsql:sync(Handle),
      handle_error(Error)
  end,

  Ret.

%%--------------------------------------------------------------------------------------------------

prepare(C, SQL) ->
  Handle = C#gen_dbi_dbh.handle,

  Ret = case pgsql:parse(Handle, SQL) of
    {ok, Statement} -> {ok, Statement};
    {error, Error}  -> handle_error(Error)
  end,

  Ret.

%%--------------------------------------------------------------------------------------------------

execute(C, Args) when is_record(C, gen_dbi_sth) ->
  Handle = C#gen_dbi_sth.handle,
  Statement = C#gen_dbi_sth.statement,

  %% TODO: error check?
  Bind = case pgsql:bind(Handle, Statement, Args) of
    ok -> ok;
    {error, BindError} -> 
      {error, BindError}
  end,

  % {ok | partial, Rows} = pgsql:execute(C, Statement, [PortalName], [MaxRows]).
  % {ok, Count}          = pgsql:execute(C, Statement, [PortalName]).
  % {ok, Count, Rows}    = pgsql:execute(C, Statement, [PortalName]).
  % All functions return {error, Error} when an error occurs.

  Ret = case Bind of
    ok ->
      case pgsql:execute(Handle, Statement) of
        {ok, _RowsOrCount}=R  -> R;
        {partial, _Rows}=R    -> R;
        {ok, _Count, _Rows}=R -> R;
        {error, Error}        ->      
          ok = pgsql:sync(Handle),
          handle_error(Error)
      end;

    {error, Error} ->
      handle_error(Error)
  end,
      
  Ret.
   
%%--------------------------------------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------------------------------------

%% TODO: add scenarios??
handle_error(Error) ->
  case Error of
    {{badmatch,{error,nxdomain}},_} -> {error, invalid_hostname};
    invalid_password                -> {error, invalid_password};
    <<"3D000">>                     -> {error, invalid_database};

    {error, error, <<"42601">>, Message, Params} -> 
      {error, invalid_syntax, [{message, Message}] ++ Params };

    {error, error, <<"23505">>, Message, Params} ->
      {error, unique_constraint, [{message, Message}] ++ Params};

    {error, error, <<"22001">>, Message, Params} ->
      {error, invalid_value, [{message, Message}] ++ Params};

    _ ->
      error_logger:error_msg("unknown gen_dbd_pg error: ~p\n", [Error]),
      {error, Error}
  end.

%%--------------------------------------------------------------------------------------------------  

%% TODO: add scenarios?
handle_exception(C, E) ->
  error_logger:error_msg(
    "unknown gen_dbd_pg:connect exception \nclass: ~p \nexception: ~p\n",[C, E]),

  {error, system_malfunction}.

%%--------------------------------------------------------------------------------------------------  

get_cols_and_rows({ok, _Count, Cols, Rows}) ->
  {Cols, Rows};
get_cols_and_rows({ok, Cols, Rows}) ->
  {Cols, Rows};
get_cols_and_rows(R) ->
  throw({invalid_result,[{result, R}]}).

%%--------------------------------------------------------------------------------------------------