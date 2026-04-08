-module(zaya_pool_test_backend).

-export([
  setup/1,
  cleanup/1,
  fail_once/2,
  events/1,
  pool_batch/2
]).

-define(TABLE, ?MODULE).

setup(Ref)->
  ensure_table(),
  cleanup(Ref),
  ok.

cleanup(Ref)->
  ensure_table(),
  ets:match_delete(?TABLE, {{fail_once, Ref, '_'}, '_'}),
  ets:match_delete(?TABLE, {{event_counter, Ref}, '_'}),
  ets:match_delete(?TABLE, {{event, Ref, '_'}, '_'}),
  ok.

fail_once(Ref, Type) when Type =:= write; Type =:= delete->
  ensure_table(),
  true = ets:insert(?TABLE, {{fail_once, Ref, Type}, true}),
  ok.

events(Ref)->
  ensure_table(),
  [
    Event
   || {{event, _Ref, _Index}, Event} <- lists:sort(ets:match_object(?TABLE, {{event, Ref, '_'}, '_'}))
  ].

pool_batch(Ref, Requests)->
  maybe_fail(Ref, Requests),
  record_event(Ref, {batch, self(), Requests}),
  ok.

maybe_fail(Ref, Requests)->
  maybe_fail(Ref, write, Requests),
  maybe_fail(Ref, delete, Requests).

maybe_fail(Ref, Type, Requests)->
  ensure_table(),
  case has_request_type(Type, Requests) of
    true ->
      case ets:take(?TABLE, {fail_once, Ref, Type}) of
        [{_, true}] ->
          exit({planned_failure, Type});
        [] ->
          ok
      end;
    false ->
      ok
  end.

has_request_type(Type, Requests)->
  lists:any(
    fun({RequestType, _Data})->
      RequestType =:= Type
    end,
    Requests
  ).

record_event(Ref, Event)->
  ensure_table(),
  Index = ets:update_counter(?TABLE, {event_counter, Ref}, 1, {{event_counter, Ref}, 0}),
  true = ets:insert(?TABLE, {{event, Ref, Index}, Event}),
  ok.

ensure_table()->
  case ets:info(?TABLE) of
    undefined ->
      ?TABLE = ets:new(?TABLE, [named_table, public, ordered_set]),
      ok;
    _ ->
      ok
  end.
