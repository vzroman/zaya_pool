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
-define(CALL_RETRY_COUNT, 10).
-define(DEFAULT_BATCH_SIZE, 1000).

start_link(Params) when is_map(Params)->
  supervisor:start_link(?MODULE, [defaults(Params)]).

call(Pool, Request)->
  call(Pool, Request, ?CALL_RETRY_COUNT).

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

call(Pool, Request, Retries) when Retries > 1->
  try
    Worker = zaya_pool_monitor:worker(Pool),
    zaya_pool_worker:call(Worker, Request)
  catch
    exit:_Reason->
      receive after 10 -> ok end,
      call(Pool, Request, Retries - 1)
  end;
call(Pool, Request, 1)->
  Worker = zaya_pool_monitor:worker(Pool),
  zaya_pool_worker:call(Worker, Request).

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

defaults(Params)->
  maps:merge(
    #{
      pool_size => erlang:system_info(logical_processors),
      batch_size => ?DEFAULT_BATCH_SIZE
    },
    Params
  ).
