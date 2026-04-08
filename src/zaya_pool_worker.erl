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
  requests = [],
  size = 0,
  reply = []
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
      collect_requests(BatchSize, append_request(From, Requests, #batch{}));
    {'EXIT', _From, Reason}->
      exit(Reason)
  end.

collect_requests(
  BatchSize,
  Batch = #batch{
    size = Size
  }
) when Size >= BatchSize->
  drain_requests(Batch);
collect_requests(BatchSize, Batch)->
  receive
    {pool_call, From, Requests}->
      collect_requests(BatchSize, append_request(From, Requests, Batch))
  after
    0 ->
      Batch
  end.

drain_requests(Batch)->
  receive
    {pool_call, From, Requests}->
      drain_requests(append_request(From, Requests, Batch))
  after
    0 ->
      Batch
  end.

append_request(
  From,
  Requests,
  Batch = #batch{
    requests = RequestsRev,
    size = Size,
    reply = ReplyTo
  }
) ->
  Batch#batch{
    requests = lists:reverse(Requests, RequestsRev),
    size = Size + request_size(Requests),
    reply = [From | ReplyTo]
  }.

request_size(Requests)->
  lists:sum(
    [
      length(Data)
     || {_Type, Data} <- Requests
    ]
  ).

flush_batch(
  #batch{
    requests = RequestsRev,
    reply = ReplyTo
  },
  Ref,
  Module
)->
  ok = Module:pool_batch(Ref, lists:reverse(RequestsRev)),
  [reply(To) || To <- lists:reverse(ReplyTo)],
  ok.

reply({Pid, Monitor})->
  Pid ! {pool_reply, Monitor, ok},
  ok.
