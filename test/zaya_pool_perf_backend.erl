-module(zaya_pool_perf_backend).

-export([
  setup/2,
  cleanup/1,
  stats/1,
  pool_batch/2
]).

-define(TABLE, ?MODULE).

setup(Ref, EntriesPerWrite)->
  ensure_table(),
  cleanup(Ref),
  _ = EntriesPerWrite,
  true = ets:insert(?TABLE, {{writes, Ref}, 0}),
  true = ets:insert(?TABLE, {{entries, Ref}, 0}),
  true = ets:insert(?TABLE, {{flushes, Ref}, 0}),
  true = ets:insert(?TABLE, {{deletes, Ref}, 0}),
  ok.

cleanup(Ref)->
  ensure_table(),
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

pool_batch(Ref, Requests)->
  ensure_table(),
  #{writes := Writes, entries := Entries, deletes := Deletes} = batch_stats(Requests),
  _ = ets:update_counter(?TABLE, {writes, Ref}, Writes),
  _ = ets:update_counter(?TABLE, {entries, Ref}, Entries),
  _ = ets:update_counter(?TABLE, {deletes, Ref}, Deletes),
  _ = ets:update_counter(?TABLE, {flushes, Ref}, 1),
  ok.

counter(Ref, Name)->
  [{_, Value}] = ets:lookup(?TABLE, {Name, Ref}),
  Value.

batch_stats(Requests)->
  lists:foldl(
    fun
      ({write, Data}, #{writes := Writes, entries := Entries, deletes := Deletes})->
        #{
          writes => Writes + 1,
          entries => Entries + length(Data),
          deletes => Deletes
        };
      ({delete, Data}, #{writes := Writes, entries := Entries, deletes := Deletes})->
        #{
          writes => Writes,
          entries => Entries,
          deletes => Deletes + length(Data)
        }
    end,
    #{
      writes => 0,
      entries => 0,
      deletes => 0
    },
    Requests
  ).

ensure_table()->
  case ets:info(?TABLE) of
    undefined ->
      ?TABLE = ets:new(?TABLE, [named_table, public, set]),
      ok;
    _ ->
      ok
  end.
