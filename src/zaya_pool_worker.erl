-module(zaya_pool_worker).

-export([
  start_link/3,
  call/2
]).

-export([
  init/5
]).

-record(state, {
  ref,
  module,
  batch_size
}).

-record(batch, {
  requests,
  size,
  reply
}).

start_link(
  #{
    ref := Ref,
    batch_size := BatchSize,
    module := Module
  },
  Index,
  Supervisor
)->
  proc_lib:start_link(?MODULE, init, [Ref, BatchSize, Module, Index, Supervisor]).

call(Worker, Requests)->
  Monitor = erlang:monitor(process, Worker),
  Worker ! {pool_call, {self(), Monitor}, Requests},
  receive
    {pool_reply, Monitor, ok}->
      erlang:demonitor(Monitor, [flush]),
      ok;
    {'DOWN', Monitor, process, Worker, Reason}->
      exit(Reason)
  end.

init(Ref, BatchSize, Module, Index, Supervisor)->
  process_flag(trap_exit, true),
  ok = zaya_pool_monitor:register_worker(Supervisor, Index, self()),
  proc_lib:init_ack({ok, self()}),
  loop(
    #state{
      ref = Ref,
      module = Module,
      batch_size = BatchSize
    }
  ).

loop(
  #state{
    batch_size = BatchSize,
    ref = Ref,
    module = Module
  } = State
)->
  Batch = collect_requests(BatchSize),
  flush_batch(Batch, Ref, Module),
  loop(State).

collect_requests(BatchSize)->
  receive
    {pool_call, From, Requests}->
      Batch = #batch{
        requests = [Requests],
        size = request_size(Requests, 0),
        reply = [From]
      },
      collect_requests(BatchSize, Batch);
    {'EXIT', _From, Reason}->
      exit(Reason)
  end.

collect_requests(
  BatchSize,
  Batch = #batch{
    size = Size
  }
) when Size >= BatchSize->
  Batch;
collect_requests(
    BatchSize,
    Batch0 = #batch{
      requests = BatchRequests,
      size = Size,
      reply = ReplyTo
    })->
  receive
    {pool_call, From, Requests}->
      Batch = Batch0#batch{
        requests = [Requests|BatchRequests],
        size = request_size(Requests, Size),
        reply = [From|ReplyTo]
      },
      collect_requests(BatchSize, Batch)
  after
    0 ->
      Batch0
  end.

request_size([{_Type, Data}|Rest], Sum)->
  request_size(Rest, Sum + length(Data));
request_size([], Sum)->
  Sum.

flush_batch(
  #batch{
    requests = Requests0,
    reply = ReplyTo
  },
  Ref,
  Module
)->
  Requests = lists:append( lists:reverse( Requests0) ),
  ok = Module:pool_batch(Ref, Requests),
  [reply(To) || To <- lists:reverse(ReplyTo)],
  ok.

reply({Pid, Monitor})->
  Pid ! {pool_reply, Monitor, ok},
  ok.
