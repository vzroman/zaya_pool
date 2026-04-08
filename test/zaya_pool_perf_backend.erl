-module(zaya_pool_perf_backend).

-export([
  setup/2,
  cleanup/1,
  stats/1,
  pool_write/2,
  pool_delete/2
]).

-define(TABLE, ?MODULE).

setup(Ref, EntriesPerWrite)->
  ensure_table(),
  cleanup(Ref),
  true = ets:insert(?TABLE, {{entries_per_write, Ref}, EntriesPerWrite}),
  true = ets:insert(?TABLE, {{writes, Ref}, 0}),
  true = ets:insert(?TABLE, {{entries, Ref}, 0}),
  true = ets:insert(?TABLE, {{flushes, Ref}, 0}),
  true = ets:insert(?TABLE, {{deletes, Ref}, 0}),
  ok.

cleanup(Ref)->
  ensure_table(),
  ets:match_delete(?TABLE, {{entries_per_write, Ref}, '_'}),
  ets:match_delete(?TABLE, {{writes, Ref}, '_'}),
  ets:match_delete(?TABLE, {{entries, Ref}, '_'}),
  ets:match_delete(?TABLE, {{flushes, Ref}, '_'}),
  ets:match_delete(?TABLE, {{deletes, Ref}, '_'}),
  ok.

stats(Ref)->
  ensure_table(),
  #{
    writes => counter(Ref, writes),
    entries => counter(Ref, entries),
    flushes => counter(Ref, flushes),
    deletes => counter(Ref, deletes)
  }.

pool_write(Ref, Data)->
  ensure_table(),
  Entries = length(Data),
  Writes = Entries div entries_per_write(Ref),
  _ = ets:update_counter(?TABLE, {writes, Ref}, Writes),
  _ = ets:update_counter(?TABLE, {entries, Ref}, Entries),
  _ = ets:update_counter(?TABLE, {flushes, Ref}, 1),
  ok.

pool_delete(Ref, Data)->
  ensure_table(),
  _ = ets:update_counter(?TABLE, {deletes, Ref}, length(Data)),
  ok.

entries_per_write(Ref)->
  [{_, Value}] = ets:lookup(?TABLE, {entries_per_write, Ref}),
  Value.

counter(Ref, Name)->
  [{_, Value}] = ets:lookup(?TABLE, {Name, Ref}),
  Value.

ensure_table()->
  case ets:info(?TABLE) of
    undefined ->
      ?TABLE = ets:new(?TABLE, [named_table, public, set]),
      ok;
    _ ->
      ok
  end.
