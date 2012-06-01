-module(ppool_serv).
-behaviour(gen_server).
-export([start/5, start_link/5, run/2, 
         sync_queue/2, async_queue/2, 
         status/1, is_busy/1, stop/1, configure/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

%% The friendly supervisor is started dynamically!
-define(SPEC(MFA),
        {worker_sup,
         {ppool_worker_sup, start_link, [MFA]},
          permanent,
          10000,
          supervisor,
          [ppool_worker_sup]}).

-record(state, {running_max=0,
                running_cnt=0,
                sup,
                refs,
                queue_max=0,
                queue_cnt=0,
                queue=queue:new(),
                sync_task=none}).

start(Name, Limit, QueueLimit, Sup, MFA) when is_atom(Name), is_integer(Limit), is_integer(QueueLimit) ->
    gen_server:start({local, Name}, ?MODULE, {Limit, QueueLimit, MFA, Sup}, []).

start_link(Name, Limit, QueueLimit, Sup, MFA) when is_atom(Name), is_integer(Limit), is_integer(QueueLimit) ->
    gen_server:start_link({local, Name}, ?MODULE, {Limit, QueueLimit, MFA, Sup}, []).

run(Name, Args) ->
    gen_server:call(Name, {run, Args}).

sync_queue(Name, Args) ->
    gen_server:call(Name, {sync, Args}, infinity).

async_queue(Name, Args) ->
    gen_server:call(Name, {async, Args}, infinity).

configure(Name, Args) ->
    gen_server:call(Name, {configure, Args}, infinity).

status(Name) ->
    gen_server:call(Name, status, infinity).

is_busy(Name) ->
    gen_server:call(Name, is_busy, infinity).

stop(Name) ->
    gen_server:call(Name, stop).

%% Gen server
init({Limit, QueueLimit, MFA, Sup}) ->
    %% We need to find the Pid of the worker supervisor from here,
    %% but alas, this would be calling the supervisor while it waits for us!
    self() ! {start_worker_supervisor, Sup, MFA},
    {ok, #state{running_max=Limit, queue_max=QueueLimit, refs=gb_sets:empty()}}.

handle_call({run, Args}, _From, S = #state{running_cnt=N, running_max=M, sup=Sup, refs=R}) when M-N > 0 ->
    {ok, Pid} = supervisor:start_child(Sup, Args),
    Ref = erlang:monitor(process, Pid),
    {reply, {ok, run, Pid}, S#state{running_cnt=N+1, refs=gb_sets:add(Ref,R)}};
handle_call({run, _Args}, _From, S=#state{running_cnt=N, running_max=M}) when M-N =< 0 ->
    {reply, {ng, noalloc}, S};

% sync queue
handle_call({sync, Args}, _From, S = #state{running_cnt=N, running_max=M, sup=Sup, refs=R}) when M-N > 0 ->
    {ok, Pid} = supervisor:start_child(Sup, Args),
    Ref = erlang:monitor(process, Pid),
    {reply, {ok, run, Pid}, S#state{running_cnt=N+1, refs=gb_sets:add(Ref,R)}};
handle_call({sync, Args}, _, S = #state{queue=Q, queue_cnt=QN, queue_max=QM}) when QM-QN > 0 ->
    {reply, {ok, queued}, S#state{queue=queue:in(Args, Q), queue_cnt=QN+1}};
handle_call({sync, Args}, From, S = #state{sync_task=none}) ->
    {noreply, S#state{sync_task={From, Args}}};
handle_call({sync, _}, _, S) ->
    {reply, {ng, queue_full}, S};

% async queue
handle_call({async, Args}, _From, S = #state{running_cnt=N, running_max=M, sup=Sup, refs=R}) when M-N > 0 ->
    {ok, Pid} = supervisor:start_child(Sup, Args),
    Ref = erlang:monitor(process, Pid),
    {reply, {ok, run, Pid}, S#state{running_cnt=N+1, refs=gb_sets:add(Ref,R)}};
handle_call({async, Args}, _, S = #state{queue=Q, queue_max=QM, queue_cnt=QN}) when QM-QN > 0 ->
    {reply, {ok, queued}, S#state{queue=queue:in(Args, Q), queue_cnt=QN+1}};
handle_call({async, _}, _, S) ->
    {reply, {ng, queue_full}, S};

% status
handle_call(status, _From, State = #state{running_max=M, running_cnt=N, queue_max=QM, queue_cnt=QN}) ->
    {reply, {ok, {{running_max, M}, {running_cnt, N}, {queue_max, QM}, {queue_cnt, QN}}}, State};

% working?
handle_call(is_busy, _From, State = #state{running_cnt=N}) ->
    {reply, {ok, N == 0}, State};

% change running_max and/or queue_max
handle_call({configure, ConfList}, _From, State) when is_list(ConfList) ->
    case update_configs(State, ConfList) of
        {ok, NewState} ->
            {reply, {ok, ConfList}, NewState};
        {invalid, Conf} ->
            {reply, {invalid, Conf}, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    {noreply, State}.


%handle_cast({async, Args}, S=#state{limit=N, sup=Sup, refs=R}) when N > 0 ->
%    {ok, Pid} = supervisor:start_child(Sup, Args),
%    Ref = erlang:monitor(process, Pid),
%    {noreply, S#state{limit=N-1, refs=gb_sets:add(Ref,R)}};
%handle_cast({async, Args}, S=#state{queue_limit=QL, queue=Q}) when QL > 0 ->
%    {noreply, S#state{queue=queue:in(Args, Q), queue_limit=QL-1}};
%handle_cast({async, Args}, S=#state{limit=N, queue=Q}) when N =< 0 ->
%    {noreply, S#state{queue=queue:in(Args,Q)}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, _}, S = #state{refs=Refs}) ->
%    io:format("received down msg~n"),
    case gb_sets:is_element(Ref, Refs) of
        true ->
            handle_down_worker(Ref, S);
        false -> %% Not our responsibility
            {noreply, S}
    end;
handle_info({start_worker_supervisor, Sup, MFA}, S = #state{}) ->
    {ok, Pid} = supervisor:start_child(Sup, ?SPEC(MFA)),
    {noreply, S#state{sup=Pid}};
handle_info(Msg, State) ->
    io:format("Unknown msg: ~p~n", [Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

update_configs(State, []) ->
    {ok, State};
update_configs(State, [Conf|Rest]) ->
    case update_one_conf(State, Conf) of
        {ok, NewState} ->
            update_configs(NewState, Rest);
        invalid ->
            {invalid, Conf}
    end.

update_one_conf(State, {running_max, N}) when is_integer(N), N > 0 ->
    {ok, State#state{running_max=N}};
update_one_conf(State, {queue_max, M}) when is_integer(M), M > 0 ->
    {ok, State#state{queue_max=M}};
update_one_conf(_State, _) ->
    invalid.

handle_down_worker(Ref, S = #state{running_cnt=N, queue_cnt=QN, sup=Sup, refs=Refs, sync_task=ST}) ->
    case queue:out(S#state.queue) of
        {{value, Args}, Q} ->
            {ok, Pid} = supervisor:start_child(Sup, Args),
            NewRef = erlang:monitor(process, Pid),
            NewRefs = gb_sets:insert(NewRef, gb_sets:delete(Ref,Refs)),
            case ST of
                none ->
                    {noreply, S#state{refs=NewRefs, queue=Q, queue_cnt=QN - 1}};
                {From, NewArgs} ->
                    gen_server:reply(From, {ok, queued}),
                    {noreply, S#state{refs=NewRefs, queue=queue:in(NewArgs, Q), sync_task=none}}
            end;
        {empty, _} ->
            {noreply, S#state{running_cnt=N - 1, refs=gb_sets:delete(Ref,Refs)}}
    end.

