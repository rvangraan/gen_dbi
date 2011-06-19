%%--------------------------------------------------------------------------------------------------
-module(gen_dbd_pg).
%%--------------------------------------------------------------------------------------------------
-export([
  connect/5,
  disconnect/1,
  execute/3,
  select_proplists/3,
  select_lists/3,
  select_records/4
]).
%%--------------------------------------------------------------------------------------------------

connect(Host, Database, Username, Password, _DBDOpts) when 
  is_list(Host), is_list(Database), is_list(Username), is_list(Password)
->
%%  {ok, P} = pgsql_connection:start_link(),
  Ret = pgsql:connect(Host, Username, Password, [{database, Database}]),
  Ret.

%%--------------------------------------------------------------------------------------------------

disconnect(C) ->
  Ret = pgsql:close(C),
  Ret.

%%--------------------------------------------------------------------------------------------------

select_proplists(C, SQL, Params) ->
  case execute(C, SQL, Params) of
    {ok, Columns, Rows} ->
      ColNames = [list_to_atom(binary_to_list(element(2, Col))) || Col <- Columns],
      loop_select_proplists(ColNames, Rows, []);

    {error, _Error}=R -> R;
    _ -> throw(invalid_select)
  end.
      
loop_select_proplists(_Columns, [], Acc) ->
  lists:reverse(Acc);
loop_select_proplists(Columns, [Row|Rows], Acc) ->
  NewRow = lists:zipwith(fun(X,Y) -> {X,Y} end, Columns, tuple_to_list(Row)),
  loop_select_proplists(Columns, Rows, [NewRow|Acc]).
  
%%--------------------------------------------------------------------------------------------------

select_lists(_C, _SQL, _Params) ->
  todo.

%%--------------------------------------------------------------------------------------------------

select_records(_C, _SQL, _Params, _RecordName) ->
  todo.

%%--------------------------------------------------------------------------------------------------

execute(C, SQL, Params) when is_list(Params), is_list(SQL) ->

  % {ok, Columns, Rows}        = pgsql:equery(C, "select ...", [Parameters]).
  % {ok, Count}                = pgsql:equery(C, "update ...", [Parameters]).
  % {ok, Count, Columns, Rows} = pgsql:equery(C, "insert ... returning ...", [Parameters]).
  % {error, Error}             = pgsql:equery(C, "invalid SQL", [Parameters]).           

  Ret = case pgsql:equery(C, SQL, Params) of
    {ok, _Columns, _Rows}=R         -> R;
    {ok, _Count}=R                  -> R;
    {ok, _Count, _Columns, _Rows}=R -> R;
    {error, _Error}=R               -> R
  end,

  Ret.

%%--------------------------------------------------------------------------------------------------