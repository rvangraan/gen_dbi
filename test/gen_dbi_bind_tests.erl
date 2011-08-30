%%% @author Rudolph van Graan <rvg@macbook-rvg.local>
%%% @copyright (C) 2011, Rudolph van Graan
%%% @doc
%%%
%%% @end
%%% Created : 30 Aug 2011 by Rudolph van Graan <rvg@macbook-rvg.local>

-module(gen_dbi_bind_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("datetime/include/datetime.hrl").

%%==============================================================================================
tag_type_test_() ->
  [?_assertMatch({string,"123"},     gen_dbi_bind:tag_type("123")),
   ?_assertMatch({binary,<<"123">>}, gen_dbi_bind:tag_type(<<"123">>)),
   ?_assertMatch({integer,123},      gen_dbi_bind:tag_type(123)),
   ?_assertMatch({float,123.3},      gen_dbi_bind:tag_type(123.3)),
   ?_assertMatch(#datetime{},        gen_dbi_bind:tag_type(#datetime{}))
  ].
%%==============================================================================================
escape_quote_test_() ->
  [?_assertMatch(<<"'123'">>,      gen_dbi_bind:escape_quote({string,"123"})),
   ?_assertMatch(<<"''''">>,       gen_dbi_bind:escape_quote({string,"'"})),
   ?_assertMatch(<<"'123'">>,      gen_dbi_bind:escape_quote({binary,<<"123">>})),
   ?_assertMatch(<<"123">>,        gen_dbi_bind:escape_quote({integer,123})),
   ?_assertMatch(<<"123.200000">>, gen_dbi_bind:escape_quote({float,123.2}))
  ].
%%==============================================================================================
escape_string_test_() ->
  [?_assertMatch(<<>>,gen_dbi_bind:escape_string(<<>>)),
   ?_assertMatch(<<"''">>,gen_dbi_bind:escape_string(<<"'">>)),
   ?_assertMatch(<<"AA">>,gen_dbi_bind:escape_string(<<"AA">>)),
   ?_assertMatch(<<"AA">>,gen_dbi_bind:escape_string("AA"))
  ].
%%==============================================================================================
simple_bind_test_() ->
  [?_assertMatch(<<"SELECT FROM WHERE a=123">>,         gen_dbi_bind:simple_bind(<<"SELECT FROM WHERE a=?">>,[123])),
   ?_assertMatch(<<"SELECT FROM WHERE a='123''4'">>,    gen_dbi_bind:simple_bind(<<"SELECT FROM WHERE a=?">>,["123'4"])),
   ?_assertMatch(<<"SELECT FROM WHERE a=?">>,           gen_dbi_bind:simple_bind(<<"SELECT FROM WHERE a=\\?">>,["123'4"])),
   ?_assertMatch(<<"SELECT FROM WHERE a=1,b=2,c='3'">>, gen_dbi_bind:simple_bind(<<"SELECT FROM WHERE a=?,b=?,c=?">>,[1,2,"3"]))
  ].
