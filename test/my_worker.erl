-module(my_worker).
-behaviour(gen_server).
-export([start_link/1, init/1, handle_call/3, handle_cast/2, 
         handle_info/2, terminate/2, code_change/3]).

start_link(Msg) ->
    gen_server:start_link(?MODULE, Msg, []).

init(Msg) ->
    self() ! {run, Msg},
    {ok, {}}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast(_, S) ->
    {noreply, S}.

handle_info({run, Msg}, State) ->
    io:format("~s", [Msg]),
    {stop, normal, State};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _State) -> 
    ok.

code_change(_, State, _) -> 
    {ok, State}.
