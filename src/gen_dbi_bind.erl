%%% @author Rudolph van Graan <rvg@macbook-rvg.local>
%%% @copyright (C) 2011, Rudolph van Graan
%%% @doc
%%%
%%% @end
%%% Created : 30 Aug 2011 by Rudolph van Graan <rvg@macbook-rvg.local>

-module(gen_dbi_bind).
-include_lib("datetime/include/datetime.hrl").
-include_lib("eunit/include/eunit.hrl").

%%=======================================================================

-export([tag_type/1,
         simple_bind/2,
         escape_quote/1,
         escape_string/1]).

%%=======================================================================
simple_bind(SQL,[]) ->
  SQL;
simple_bind(SQL,Params) when is_list(SQL), is_list(Params) ->
  TaggedParams = [ tag_type(Param) || Param <- Params],
  bind_params(lists:flatten(SQL),TaggedParams);
simple_bind(SQL,Params) when is_binary(SQL), is_list(Params) ->
  TaggedParams = [ tag_type(Param) || Param <- Params],
  bind_params(SQL,TaggedParams).

%%=======================================================================
bind_params(<<"\\?",SQL/binary>>,Params) ->
  Value = <<"?">>,
  Rest = bind_params(SQL,Params),
  <<Value/binary,Rest/binary>>;
bind_params(<<"?",SQL/binary>>,[Param|Params]) ->
  BoundParam = escape_quote(Param),
  Rest = bind_params(SQL,Params),
  <<BoundParam/binary,Rest/binary>>;
bind_params(<<C:8,SQL/binary>>,Params) ->
  Rest = bind_params(SQL,Params),
  <<C:8,Rest/binary>>;
bind_params(<<>>,_Params) ->
  <<>>.



%%=======================================================================
escape_quote({binary,Binary}) when is_binary(Binary) ->
  <<"'",Binary/binary,"'">>;
escape_quote({string,String}) when is_list(String) ->
  B = escape_string(String),
  <<"'",B/binary,"'">>;
escape_quote({integer,Integer}) when is_integer(Integer) ->
  list_to_binary(integer_to_list(Integer));
escape_quote({float,Float}) when is_float(Float) ->
  list_to_binary(io_lib:format("~f",[Float])).
  
%%=======================================================================  
ensure_pure_string(S) ->
  ok.
%%=======================================================================  
escape_string(<<"'",Rest/binary>>) ->
  Body = escape_string(Rest),
  <<"''",Body/binary>>;
escape_string(<<C:8,Rest/binary>>) ->
  Body = escape_string(Rest),
  <<C:8,Body/binary>>;
escape_string(<<>>) ->
  <<>>;
escape_string(String) when is_list(String) ->
  escape_string(list_to_binary(String)).


%%=======================================================================  
tag_type(Integer) when is_integer(Integer) ->
  {integer,Integer};
tag_type(String) when is_list(String) ->
  {string,String};
tag_type(Binary) when is_binary(Binary) ->
  {binary,Binary};
tag_type(Float) when is_float(Float) ->
  {float,Float};
tag_type(#datetime{} = Date) ->
  Date.

