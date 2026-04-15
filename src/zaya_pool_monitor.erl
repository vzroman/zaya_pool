-module(zaya_pool_monitor).

-behaviour(gen_server).

-export([
  start_link/2,
  worker/1,
  register_worker/3
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state, {
  supervisor
}).

-define(STORAGE_KEY(Supervisor), {zaya_pool, Supervisor}).

start_link(Supervisor, Params)->
  gen_server:start_link(?MODULE, [Supervisor, Params], []).

worker(Supervisor)->
  #{
    size := Size,
    workers := Workers
  } = storage(Supervisor),
  Index = (erlang:system_time(millisecond) rem Size) + 1,
  element(Index, Workers).

register_worker(Supervisor, Index, Worker)->
  gen_server:call(monitor(Supervisor), {register_worker, Index, Worker}, infinity).

init([Supervisor, #{pool_size := Size}])->
  process_flag(trap_exit, true),
  persistent_term:put(
    ?STORAGE_KEY(Supervisor),
    #{
      monitor => self(),
      workers => erlang:make_tuple(Size, undefined),
      size => Size
    }
  ),
  {ok, #state{supervisor = Supervisor}}.

handle_call({register_worker, Index, Worker}, _From, State)->
  {reply, ok, register_worker_pid(Index, Worker, State)}.

handle_cast(_Request, State)->
  {noreply, State}.

handle_info(_Info, State)->
  {noreply, State}.

terminate(_Reason, #state{supervisor = Supervisor})->
  persistent_term:erase(?STORAGE_KEY(Supervisor)),
  ok.

code_change(_OldVsn, State, _Extra)->
  {ok, State}.

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

storage(Supervisor)->
  persistent_term:get(?STORAGE_KEY(Supervisor)).

monitor(Supervisor)->
  case storage(Supervisor) of
    #{monitor := Monitor} when is_pid(Monitor)->
      Monitor;
    _ ->
      exit({pool_monitor_not_found, Supervisor})
  end.
