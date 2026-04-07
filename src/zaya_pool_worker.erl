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
  max_size
}).

-record(batch, {
  type,
  data,
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
    {pool_reply, Monitor, Reply}->
      erlang:demonitor(Monitor, [flush]),
      handle_reply(Reply);
    {'DOWN', Monitor, process, Worker, Reason}->
      exit(Reason)
  end.

init(Ref, BatchSize, Module, Index, Supervisor)->
  ok = zaya_pool_monitor:register_worker(Supervisor, Index, self()),
  proc_lib:init_ack({ok, self()}),
  loop(
    undefined,
    #state{
      ref = Ref,
      module = Module,
      max_size = BatchSize
    }
  ).

loop(
  PendingBatch,
  #state{
    max_size = MaxSize,
    ref = Ref,
    module = Module
  } = State
)->
  {Batches, NextBatch} = collect_requests(MaxSize, PendingBatch),
  flush_batches(Batches, Ref, Module),
  loop(NextBatch, State).

collect_requests(MaxSize, undefined)->
  receive
    {pool_call, From, Requests}->
      case merge_requests(From, Requests, undefined) of
        [Batch] ->
          collect_requests(MaxSize, Batch);
        Batches0 ->
          split_batches(Batches0)
      end
  end;
collect_requests(
  MaxSize,
  Batch0 = #batch{
    size = BatchSize
  }
) when BatchSize < MaxSize->
  receive
    {pool_call, From, Requests}->
      case merge_requests(From, Requests, Batch0) of
        [Batch] ->
          collect_requests(MaxSize, Batch);
        Batches0 ->
          split_batches(Batches0)
      end
  after
    0 ->
      {[Batch0], undefined}
  end;
collect_requests(_MaxSize, Batch = #batch{})->
  {[Batch], undefined}.

split_batches(Batches0)->
  {Batches, [Next]} = lists:split(length(Batches0) - 1, Batches0),
  {Batches, Next}.

merge_requests(From, [{Type, Data} | Rest], undefined)->
  Batch = #batch{
    type = Type,
    data = [Data],
    size = length(Data),
    reply = []
  },
  merge_requests(From, Rest, Batch);
merge_requests(
  From,
  [{Type, Data} | Rest],
  Batch0 = #batch{
    type = BatchType,
    size = BatchSize,
    data = BatchData
  }
) ->
  if
    Type =:= BatchType ->
      Batch = Batch0#batch{
        size = BatchSize + length(Data),
        data = [Data | BatchData]
      },
      merge_requests(From, Rest, Batch);
    true ->
      NextBatch = #batch{
        type = Type,
        data = [Data],
        size = length(Data),
        reply = []
      },
      [Batch0 | merge_requests(From, Rest, NextBatch)]
  end;
merge_requests(
  From,
  [],
  Batch0 = #batch{
    reply = ReplyTo
  }
) ->
  [Batch0#batch{reply = [From | ReplyTo]}].

flush_batches([Batch | Rest], Ref, Module)->
  Reply =
    try
      flush_batch(Batch, Ref, Module),
      ok
    catch
      Class:Reason:Stack->
        {raise, Class, Reason, Stack}
    end,
  reply_batch(Batch, Reply),
  flush_batches(Rest, Ref, Module);
flush_batches([], _Ref, _Module)->
  ok.

flush_batch(
  #batch{
    type = write,
    data = DataList
  },
  Ref,
  Module
)->
  ok = Module:pool_write(Ref, merged_data(DataList));
flush_batch(
  #batch{
    type = delete,
    data = DataList
  },
  Ref,
  Module
)->
  ok = Module:pool_delete(Ref, merged_data(DataList)).

merged_data([Data])->
  Data;
merged_data(DataList)->
  lists:append(lists:reverse(DataList)).

reply_batch(#batch{reply = ReplyTo}, Reply)->
  lists:foreach(
    fun(From)->
      reply(From, Reply)
    end,
    lists:reverse(ReplyTo)
  ),
  ok.

reply({Pid, Monitor}, Reply)->
  Pid ! {pool_reply, Monitor, Reply},
  ok.

handle_reply({raise, Class, Reason, Stack})->
  erlang:raise(Class, Reason, Stack);
handle_reply(Reply)->
  Reply.
