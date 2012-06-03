%%% API module for the pool
-module(ppool).
-behaviour(application).
-export([start/2, stop/1, start_pool/4,
         run/2, sync_queue/2, async_queue/2, 
         status/1, is_busy/1, wait_for/1,
         stop_pool/1, configure/2]).

start(normal, _Args) ->
    ppool_supersup:start_link().

stop(_State) ->
    ok.

start_pool(Name, Limit, QueueLimit, {M,F,A}) ->
    ppool_supersup:start_pool(Name, Limit, QueueLimit, {M,F,A}).

stop_pool(Name) ->
    ppool_supersup:stop_pool(Name).

run(Name, Args) ->
    ppool_serv:run(Name, Args).

async_queue(Name, Args) ->
    ppool_serv:async_queue(Name, Args).

sync_queue(Name, Args) ->
    ppool_serv:sync_queue(Name, Args).

configure(Name, Args) ->
    ppool_serv:configure(Name, Args).

status(Name) ->
    ppool_serv:status(Name).

is_busy(Name) ->
    ppool_serv:is_busy(Name).

wait_for(Name) ->
    case ppool_serv:is_busy(Name) of
        {ok, true} ->
            timer:sleep(1000),
            wait_for(Name);
        {ok, false} ->
            ok
    end.
