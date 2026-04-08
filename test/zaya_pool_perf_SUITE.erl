-module(zaya_pool_perf_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
  suite/0,
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_group/2,
  end_per_group/2,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  concurrent_writes_benchmark/1
]).

-define(SMOKE_PROCESSES, 100).
-define(SMOKE_WRITES_PER_PROCESS, 100).
-define(DEFAULT_PROCESSES, 10000).
-define(DEFAULT_WRITES_PER_PROCESS, 10000).
-define(DEFAULT_ENTRIES_PER_WRITE, 10).
-define(DEFAULT_READY_TIMEOUT_MS, 3600000).
-define(DEFAULT_COMPLETION_TIMEOUT_MS, 3600000).
-define(DEFAULT_POOL_SIZE, 16).
-define(DEFAULT_BATCH_SIZE, 1000).

suite()->
  [{timetrap, {hours, 3}}].

all()->
  [{group, smoke_concurrent_writes_benchmark}].

groups()->
  [
    {
      smoke_concurrent_writes_benchmark,
      [],
      [concurrent_writes_benchmark]
    },
    {
      massive_concurrent_writes_benchmark,
      [],
      [concurrent_writes_benchmark]
    }
  ].

init_per_suite(Config)->
  Config.

end_per_suite(_Config)->
  ok.

init_per_group(smoke_concurrent_writes_benchmark, Config)->
  [
    {benchmark_group, smoke_concurrent_writes_benchmark},
    {benchmark_config, benchmark_config(smoke)}
    | Config
  ];
init_per_group(massive_concurrent_writes_benchmark, Config)->
  [
    {benchmark_group, massive_concurrent_writes_benchmark},
    {benchmark_config, benchmark_config(massive)}
    | Config
  ];
init_per_group(_, Config)->
  Config.

end_per_group(_, _Config)->
  ok.

init_per_testcase(concurrent_writes_benchmark, Config)->
  BenchmarkConfig = ?config(benchmark_config, Config),
  Ref = make_ref(),
  ok = zaya_pool_perf_backend:setup(Ref, maps:get(entries_per_write, BenchmarkConfig)),
  [{ref, Ref} | Config];
init_per_testcase(_, Config)->
  Config.

end_per_testcase(concurrent_writes_benchmark, Config)->
  ok = zaya_pool_perf_backend:cleanup(?config(ref, Config));
end_per_testcase(_, _Config)->
  ok.

concurrent_writes_benchmark(Config)->
  BenchmarkConfig = ?config(benchmark_config, Config),
  Ref = ?config(ref, Config),
  {ok, Pool} = zaya_pool:start_link(pool_params(Ref, BenchmarkConfig)),
  try
    Batches =
      prepare_batches(
        maps:get(processes, BenchmarkConfig),
        maps:get(entries_per_write, BenchmarkConfig)
      ),
    Workers = start_workers(self(), Pool, Batches, maps:get(writes_per_process, BenchmarkConfig)),
    ReadyWorkers =
      await_ready(
        Workers,
        maps:get(ready_timeout_ms, BenchmarkConfig)
      ),
    StartNative = erlang:monotonic_time(),
    [Pid ! benchmark_start || {Pid, _Monitor} <- ReadyWorkers],
    ok =
      await_done(
        ReadyWorkers,
        maps:get(completion_timeout_ms, BenchmarkConfig)
      ),
    Result =
      result(
        BenchmarkConfig,
        zaya_pool_perf_backend:stats(Ref),
        erlang:monotonic_time() - StartNative
      ),
    write_result(
      Result,
      ?config(priv_dir, Config),
      ?config(benchmark_group, Config)
    ),
    print_result(Result),
    assert_result(BenchmarkConfig, Result)
  after
    ok = zaya_pool:stop(Pool)
  end.

benchmark_config(smoke)->
  #{
    processes => ?SMOKE_PROCESSES,
    writes_per_process => ?SMOKE_WRITES_PER_PROCESS,
    entries_per_write => ?DEFAULT_ENTRIES_PER_WRITE,
    ready_timeout_ms => ?DEFAULT_READY_TIMEOUT_MS,
    completion_timeout_ms => ?DEFAULT_COMPLETION_TIMEOUT_MS,
    pool_size => ?DEFAULT_POOL_SIZE,
    batch_size => ?DEFAULT_BATCH_SIZE
  };
benchmark_config(massive)->
  #{
    processes => ?DEFAULT_PROCESSES,
    writes_per_process => ?DEFAULT_WRITES_PER_PROCESS,
    entries_per_write => ?DEFAULT_ENTRIES_PER_WRITE,
    ready_timeout_ms => ?DEFAULT_READY_TIMEOUT_MS,
    completion_timeout_ms => ?DEFAULT_COMPLETION_TIMEOUT_MS,
    pool_size => ?DEFAULT_POOL_SIZE,
    batch_size => ?DEFAULT_BATCH_SIZE
  }.

pool_params(Ref, BenchmarkConfig)->
  #{
    ref => Ref,
    pool_size => maps:get(pool_size, BenchmarkConfig),
    batch_size => maps:get(batch_size, BenchmarkConfig),
    module => zaya_pool_perf_backend
  }.

prepare_batches(Processes, EntriesPerWrite)->
  [
    prepare_batch(ProcessIndex, EntriesPerWrite)
   || ProcessIndex <- lists:seq(1, Processes)
  ].

prepare_batch(ProcessIndex, EntriesPerWrite)->
  [
    {{ProcessIndex, EntryIndex}, {ProcessIndex, EntryIndex}}
   || EntryIndex <- lists:seq(1, EntriesPerWrite)
  ].

start_workers(Parent, Pool, Batches, WritesPerProcess)->
  [
    begin
      {Pid, Monitor} =
        erlang:spawn_monitor(fun()->
          worker_loop(Parent, Pool, Batch, WritesPerProcess)
        end),
      {Pid, Monitor}
    end
   || Batch <- Batches
  ].

worker_loop(Parent, Pool, Batch, WritesPerProcess)->
  Parent ! {worker_ready, self()},
  receive
    benchmark_start ->
      ok = write_loop(Pool, Batch, WritesPerProcess),
      Parent ! {worker_done, self()}
  end.

write_loop(_Pool, _Batch, 0)->
  ok;
write_loop(Pool, Batch, WritesLeft)->
  ok = zaya_pool:call(Pool, [{write, Batch}]),
  write_loop(Pool, Batch, WritesLeft - 1).

await_ready(Workers, Timeout)->
  await_ready(Workers, Timeout, []).

await_ready([], _Timeout, Ready)->
  Ready;
await_ready(Workers, Timeout, Ready)->
  receive
    {worker_ready, Pid} ->
      {Pid, Monitor} = take_worker(Pid, Workers),
      await_ready(lists:keydelete(Pid, 1, Workers), Timeout, [{Pid, Monitor} | Ready]);
    {'DOWN', Monitor, process, Pid, Reason} ->
      ct:fail({worker_ready_down, Pid, Monitor, Reason})
  after
    Timeout ->
      ct:fail({worker_ready_timeout, length(Workers)})
  end.

await_done([], _Timeout)->
  ok;
await_done(Workers, Timeout)->
  receive
    {worker_done, Pid} ->
      await_done(lists:keydelete(Pid, 1, Workers), Timeout);
    {'DOWN', _Monitor, process, Pid, Reason} ->
      case lists:keyfind(Pid, 1, Workers) of
        {Pid, _WorkerMonitor} ->
          ct:fail({worker_done_down, Pid, Reason});
        false ->
          await_done(Workers, Timeout)
      end
  after
    Timeout ->
      ct:fail({worker_done_timeout, length(Workers)})
  end.

take_worker(Pid, Workers)->
  case lists:keyfind(Pid, 1, Workers) of
    {Pid, _Monitor} = Worker ->
      Worker;
    false ->
      ct:fail({unknown_worker, Pid})
  end.

result(BenchmarkConfig, Stats, ElapsedNative)->
  Processes = maps:get(processes, BenchmarkConfig),
  WritesPerProcess = maps:get(writes_per_process, BenchmarkConfig),
  EntriesPerWrite = maps:get(entries_per_write, BenchmarkConfig),
  TotalWrites = Processes * WritesPerProcess,
  TotalEntries = TotalWrites * EntriesPerWrite,
  ElapsedUs = erlang:convert_time_unit(ElapsedNative, native, microsecond),
  ElapsedSeconds = ElapsedUs / 1000000,
  #{
    processes => Processes,
    writes_per_process => WritesPerProcess,
    entries_per_write => EntriesPerWrite,
    pool_size => maps:get(pool_size, BenchmarkConfig),
    batch_size => maps:get(batch_size, BenchmarkConfig),
    total_requested_writes => TotalWrites,
    total_requested_entries => TotalEntries,
    backend_writes => maps:get(writes, Stats),
    backend_entries => maps:get(entries, Stats),
    backend_flushes => maps:get(flushes, Stats),
    elapsed_us => ElapsedUs,
    elapsed_seconds => ElapsedSeconds,
    writes_per_second => per_second(TotalWrites, ElapsedUs),
    entries_per_second => per_second(TotalEntries, ElapsedUs)
  }.

assert_result(BenchmarkConfig, Result)->
  ExpectedWrites =
    maps:get(processes, BenchmarkConfig) * maps:get(writes_per_process, BenchmarkConfig),
  ExpectedEntries = ExpectedWrites * maps:get(entries_per_write, BenchmarkConfig),
  ?assertEqual(ExpectedWrites, maps:get(total_requested_writes, Result)),
  ?assertEqual(ExpectedEntries, maps:get(total_requested_entries, Result)),
  ?assertEqual(ExpectedWrites, maps:get(backend_writes, Result)),
  ?assertEqual(ExpectedEntries, maps:get(backend_entries, Result)),
  ?assert(maps:get(backend_flushes, Result) > 0),
  ok.

per_second(_Count, 0)->
  infinity;
per_second(Count, ElapsedUs)->
  Count * 1000000 / ElapsedUs.

write_result(Result, PrivDir, Group)->
  Filename = filename:join(PrivDir, atom_to_list(Group) ++ "_result.term"),
  ok = file:write_file(Filename, io_lib:format("~p.~n", [Result])).

print_result(Result)->
  ct:pal(
    "zaya_pool perf result:~n  processes=~p~n  writes_per_process=~p~n  entries_per_write=~p~n  pool_size=~p~n  batch_size=~p~n  total_requested_writes=~p~n  total_requested_entries=~p~n  backend_flushes=~p~n  elapsed_seconds=~p~n  writes_per_second=~p~n  entries_per_second=~p",
    [
      maps:get(processes, Result),
      maps:get(writes_per_process, Result),
      maps:get(entries_per_write, Result),
      maps:get(pool_size, Result),
      maps:get(batch_size, Result),
      maps:get(total_requested_writes, Result),
      maps:get(total_requested_entries, Result),
      maps:get(backend_flushes, Result),
      maps:get(elapsed_seconds, Result),
      maps:get(writes_per_second, Result),
      maps:get(entries_per_second, Result)
    ]
  ).
