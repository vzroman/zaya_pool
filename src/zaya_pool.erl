-module(zaya_pool).

-behaviour(supervisor).

-export([
  start_link/1,
  call/2,
  stop/1
]).

-export([
  init/1
]).

-define(MAX_RESTARTS, 10).
-define(MAX_PERIOD, 1000).
-define(STOP_TIMEOUT, 5000).
-define(WORKER_RETRY_COUNT, 10).
-define(STORAGE_KEY(Supervisor), {?MODULE, Supervisor}).

start_link(#{pool_size := _PoolSize} = Params)->
  supervisor:start_link(?MODULE, [Params]).

call(Pool, Request)->
  Worker = worker(Pool),
  zaya_pool_worker:call(Worker, Request).

stop(Supervisor) when is_pid(Supervisor)->
  case is_process_alive(Supervisor) of
    true->
      unlink(Supervisor),
      Monitor = erlang:monitor(process, Supervisor),
      exit(Supervisor, shutdown),
      receive
        {'DOWN', Monitor, process, Supervisor, _Reason}->
          ok
      end;
    false->
      ok
  end.

init([Params]) ->
  #{pool_size := PoolSize} = Params,
  Children =
    [
      #{
        id => zaya_pool_monitor,
        start => {zaya_pool_monitor, start_link, [self(), Params]},
        restart => permanent,
        shutdown => ?STOP_TIMEOUT,
        type => worker,
        modules => [zaya_pool_monitor]
      }
      | worker_specs(Params, PoolSize)
    ],

  Supervisor = #{
    strategy => one_for_one,
    intensity => ?MAX_RESTARTS,
    period => ?MAX_PERIOD
  },

  {ok, {Supervisor, Children}}.

worker(Pool)->
  #{
    counter := Counter,
    size := Size
  } = storage(Pool),
  Index = ((atomics:add_get(Counter, 1, 1) - 1) rem Size) + 1,
  worker(Pool, Index, ?WORKER_RETRY_COUNT).

worker(Pool, Index, Retries) when Retries > 0->
  #{
    workers := Workers
  } = storage(Pool),
  case element(Index, Workers) of
    Worker when is_pid(Worker) ->
      case is_process_alive(Worker) of
        true ->
          Worker;
        false ->
          yield(),
          worker(Pool, Index, Retries - 1)
      end;
    _ ->
      yield(),
      worker(Pool, Index, Retries - 1)
  end;
worker(_Pool, Index, 0)->
  exit({pool_worker_not_ready, Index}).

storage(Pool)->
  persistent_term:get(?STORAGE_KEY(Pool)).

yield()->
  receive
  after
    0 ->
      ok
  end.

worker_specs(Params, PoolSize)->
  [
    #{
      id => {worker, Index},
      start => {zaya_pool_worker, start_link, [Params, Index, self()]},
      restart => permanent,
      shutdown => ?STOP_TIMEOUT,
      type => worker,
      modules => [zaya_pool_worker]
    }
   || Index <- lists:seq(1, PoolSize)
  ].
