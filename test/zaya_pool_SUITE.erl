-module(zaya_pool_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
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
  registration_test/1,
  write_delete_test/1,
  batching_test/1,
  retry_after_crash_test/1,
  stop_purges_storage_test/1
]).

all()->
  [
    registration_test,
    write_delete_test,
    batching_test,
    retry_after_crash_test,
    stop_purges_storage_test
  ].

groups()->
  [].

init_per_suite(Config)->
  Config.

end_per_suite(_Config)->
  ok.

init_per_group(_, Config)->
  Config.

end_per_group(_, _Config)->
  ok.

init_per_testcase(_, Config)->
  Ref = make_ref(),
  ok = zaya_pool_test_backend:setup(Ref),
  [{ref, Ref} | Config].

end_per_testcase(_, Config)->
  Ref = ?config(ref, Config),
  ok = zaya_pool_test_backend:cleanup(Ref).

registration_test(Config)->
  with_pool(
    Config,
    #{pool_size => 3},
    fun(Pool)->
      WorkerTuple = workers(Pool),
      WorkerList = tuple_to_list(WorkerTuple),

      ?assertEqual(3, length(WorkerList)),
      lists:foreach(
        fun(Worker)->
          ?assert(is_pid(Worker))
        end,
        WorkerList
      ),

      Picked = [zaya_pool_monitor:worker(Pool) || _ <- lists:seq(1, 6)],

      ?assertEqual(
        lists:sort(WorkerList),
        lists:sort(lists:usort(Picked))
      )
    end
  ).

write_delete_test(Config)->
  Ref = ?config(ref, Config),
  with_pool(
    Config,
    #{pool_size => 1},
    fun(Pool)->
      ok = zaya_pool:call(Pool, [{write, [{k1, v1}, {k2, v2}]}]),
      ok = zaya_pool:call(Pool, [{delete, [k1]}]),

      ?assertEqual(
        [
          {batch, [{write, [{k1, v1}, {k2, v2}]}]},
          {batch, [{delete, [k1]}]}
        ],
        strip_workers(zaya_pool_test_backend:events(Ref))
      )
    end
  ).

batching_test(Config)->
  Ref = ?config(ref, Config),
  with_pool(
    Config,
    #{
      pool_size => 1,
      batch_size => 16
    },
    fun(Pool)->
      Worker = element(1, workers(Pool)),
      Monitor1 = make_ref(),
      Monitor2 = make_ref(),
      Monitor3 = make_ref(),

      Worker ! {pool_call, {self(), Monitor1}, [{write, [{k1, v1}]}]},
      Worker ! {pool_call, {self(), Monitor2}, [{write, [{k2, v2}]}]},
      Worker ! {pool_call, {self(), Monitor3}, [{write, [{k3, v3}]}]},

      ok = wait_reply(Monitor1),
      ok = wait_reply(Monitor2),
      ok = wait_reply(Monitor3),

      ?assertEqual(
        [
          {
            batch,
            [
              {write, [{k1, v1}]},
              {write, [{k2, v2}]},
              {write, [{k3, v3}]}
            ]
          }
        ],
        strip_workers(zaya_pool_test_backend:events(Ref))
      )
    end
  ).

retry_after_crash_test(Config)->
  Ref = ?config(ref, Config),
  with_pool(
    Config,
    #{pool_size => 1},
    fun(Pool)->
      Worker0 = element(1, workers(Pool)),

      ok = zaya_pool_test_backend:fail_once(Ref, write),
      ok = zaya_pool:call(Pool, [{write, [{retry_key, retry_value}]}]),

      ?assert(
        wait_until(
          fun()->
            Worker1 = element(1, workers(Pool)),
            is_pid(Worker1) andalso Worker1 =/= Worker0
          end,
          50
        )
      ),

      ?assertEqual(
        [
          {batch, [{write, [{retry_key, retry_value}]}]}
        ],
        strip_workers(zaya_pool_test_backend:events(Ref))
      )
    end
  ).

stop_purges_storage_test(Config)->
  Ref = ?config(ref, Config),
  Params = params(Ref, #{}),
  {ok, Pool} = zaya_pool:start_link(Params),
  Key = storage_key(Pool),

  ?assertMatch(
    #{
      workers := _
    },
    persistent_term:get(Key)
  ),

  ok = zaya_pool:stop(Pool),
  ok = zaya_pool:stop(Pool),

  ?assertError(badarg, persistent_term:get(Key)).

with_pool(Config, Overrides, Fun)->
  Ref = ?config(ref, Config),
  {ok, Pool} = zaya_pool:start_link(params(Ref, Overrides)),
  try
    Fun(Pool)
  after
    ok = zaya_pool:stop(Pool)
  end.

params(Ref, Overrides)->
  maps:merge(
    #{
      ref => Ref,
      pool_size => 1,
      batch_size => 8,
      module => zaya_pool_test_backend
    },
    Overrides
  ).

workers(Pool)->
  maps:get(workers, persistent_term:get(storage_key(Pool))).

storage_key(Pool)->
  {zaya_pool, Pool}.

strip_workers(Events)->
  [
    {Type, Data}
   || {Type, _Worker, Data} <- Events
  ].

wait_reply(Monitor)->
  receive
    {pool_reply, Monitor, ok}->
      ok
  after
    5000 ->
      ct:fail({reply_timeout, Monitor})
  end.

wait_until(Check, Attempts) when Attempts > 0->
  case Check() of
    true ->
      true;
    false ->
      receive after 20 -> ok end,
      wait_until(Check, Attempts - 1)
  end;
wait_until(Check, 0)->
  Check().
