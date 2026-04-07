-module(zaya_pool_monitor).

-behaviour(gen_server).

-export([
  start_link/2,
  await_ready/1,
  register_worker/3
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_continue/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state, {
  supervisor,
  params,
  size,
  ready = false,
  waiters = []
}).

-define(STORAGE_KEY(Supervisor), {zaya_pool, Supervisor}).
-define(STOP_TIMEOUT, 5000).

start_link(Supervisor, Params)->
  gen_server:start_link(?MODULE, [Supervisor, Params], []).

await_ready(Supervisor)->
  gen_server:call(monitor(Supervisor), ready, infinity).

register_worker(Supervisor, Index, Worker)->
  gen_server:cast(monitor(Supervisor), {register_worker, Index, Worker}),
  ok.

init([Supervisor, #{pool_size := Size} = Params])->
  persistent_term:put(
    ?STORAGE_KEY(Supervisor),
    #{
      monitor => self(),
      workers => erlang:make_tuple(Size, undefined),
      counter => atomics:new(1, [{signed, false}]),
      size => Size
    }
  ),
  {ok,
    #state{
      supervisor = Supervisor,
      params = Params,
      size = Size
    },
    {continue, start_workers}}.

handle_call(ready, _From, #state{ready = true} = State)->
  {reply, ok, State};
handle_call(ready, From, #state{waiters = Waiters} = State)->
  {noreply, State#state{waiters = [From | Waiters]}}.

handle_cast({register_worker, Index, Worker}, State)->
  {noreply, register_worker_pid(Index, Worker, State)}.

handle_continue(start_workers, State0)->
  case start_workers(State0) of
    {ok, State1}->
      {noreply, mark_ready(State1)};
    {error, Reason}->
      {stop, Reason, State0}
  end.

handle_info(_Info, State)->
  {noreply, State}.

terminate(_Reason, #state{supervisor = Supervisor})->
  persistent_term:erase(?STORAGE_KEY(Supervisor)),
  ok.

code_change(_OldVsn, State, _Extra)->
  {ok, State}.

start_workers(#state{size = Size} = State)->
  start_workers(1, Size, State).

start_workers(Index, Size, State) when Index =< Size->
  case ensure_worker(Index, State) of
    {ok, State1}->
      start_workers(Index + 1, Size, State1);
    {error, _Reason} = Error->
      Error
  end;
start_workers(_Index, _Size, State)->
  {ok, State}.

ensure_worker(Index, #state{supervisor = Supervisor} = State)->
  case supervisor:start_child(Supervisor, worker_child_spec(Index, State)) of
    {ok, Worker}->
      {ok, register_worker_pid(Index, Worker, State)};
    {ok, Worker, _Info}->
      {ok, register_worker_pid(Index, Worker, State)};
    {error, {already_started, Worker}} when is_pid(Worker)->
      {ok, register_worker_pid(Index, Worker, State)};
    {error, already_present}->
      case worker_pid(Supervisor, Index) of
        {ok, Worker}->
          {ok, register_worker_pid(Index, Worker, State)};
        error->
          {error, {worker_not_found, Index}}
      end;
    {error, Reason}->
      {error, {start_worker_failed, Index, Reason}}
  end.

worker_child_spec(Index, #state{params = Params, supervisor = Supervisor})->
  #{
    id => {worker, Index},
    start => {zaya_pool_worker, start_link, [Params, Index, Supervisor]},
    restart => permanent,
    shutdown => ?STOP_TIMEOUT,
    type => worker,
    modules => [zaya_pool_worker]
  }.

worker_pid(Supervisor, Index)->
  case lists:keyfind({worker, Index}, 1, supervisor:which_children(Supervisor)) of
    {{worker, Index}, Worker, worker, _Modules} when is_pid(Worker)->
      {ok, Worker};
    _->
      error
  end.

register_worker_pid(Index, Worker, #state{supervisor = Supervisor} = State)->
  #{
    workers := Workers
  } = Storage = persistent_term:get(?STORAGE_KEY(Supervisor)),
  persistent_term:put(
    ?STORAGE_KEY(Supervisor),
    Storage#{
      workers => setelement(Index, Workers, Worker)
    }
  ),
  State.

mark_ready(#state{waiters = Waiters} = State)->
  lists:foreach(
    fun(From)->
      gen_server:reply(From, ok)
    end,
    lists:reverse(Waiters)
  ),
  State#state{
    ready = true,
    waiters = []
  }.

monitor(Supervisor)->
  case persistent_term:get(?STORAGE_KEY(Supervisor)) of
    #{monitor := Monitor} when is_pid(Monitor)->
      Monitor;
    _ ->
      exit({pool_monitor_not_found, Supervisor})
  end.
