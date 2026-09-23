# Junify reliability contract (SC-5538)

This fork starts at GitLab commit
`ac8620d4f1b6fcfbc3f918778557bc30c243d3ba` (Sidekiq 6 compatible upstream,
including its synchronous startup heartbeat fix). Original LGPL-3.0 copyright
and license notices remain applicable. Junify modifications start 2026-09-23.

## Domain operation contract

- Operation: retain fetched work until acknowledgment; recover interrupted work
  without removing its only Redis copy; atomically promote due retry/schedule
  entries to ready queues.
- Trigger/caller: internal Sidekiq workers and scheduler using trusted Redis
  credentials. No new customer API, authorization decision or tenant lookup.
- Risk: shared asynchronous execution, partial external effects, concurrent
  recovery and interrupted fan-out. Domain jobs still own authorization,
  callbacks, counters, audit events and idempotency. No domain SQL/schema change.
- User-visible contract: interrupted work remains inspectable. Junify defaults
  to saving interruptions in Sidekiq Dead for manual inspection; automatic
  crash replay requires a separate explicit policy. Ordinary exception retries
  remain owned by Active Job/Sidekiq and SC-5529, not inferred as crash safety.
- Race strategy: compare original serialized payload and source membership
  inside Redis Lua, write destination before deleting source, and recheck the
  old worker heartbeat during orphan recovery. Payloads are parsed/serialized
  in Ruby, never round-tripped through Lua JSON numbers. Duplicate external
  effects remain possible; this is not an exactly-once executor.
- Bounds: fetching checks at most the configured queue count per poll. Recovery
  processes bounded batches per abandoned queue; scan overhead depends on Redis
  key count. Scheduler promotions are bounded batches. No extra domain DB calls.
- Middleware: scheduled promotion requires an empty worker client middleware
  chain, enforced before execution; do not infer stock middleware compatibility.
- Dirty state: malformed payloads remain at source with a diagnostic; failed
  destination writes must preserve source. No live queue cleanup/replay.
- Rollout: Rails PR depends on SC-5529; all workers must be upgraded. Old workers
  retain BasicFetch until replaced. Rollback must drain/recover private working
  queues before removing the fork. Redis persistence/failover/eviction and
  database-commit/enqueue atomicity remain separate operational responsibilities.
- Failure/observability: save interruption reason and original job arguments in
  Dead; logs identify job/queue, not job arguments. Existing Dead retention
  applies; this is not permanent archival. No new frontend message contract.
- Permissions/markers: no new policy action, service/controller or public caller.
- Independent fresh-context review required before handoff.

## Behavioral evidence matrix

| Invariant / owner | Reachable paths | Falsifiable evidence |
| --- | --- | --- |
| Fetch retains work / fetcher | all configured queues, normal completion, SIGKILL | real worker kill, surviving private payload, recovery and next work |
| Recovery never removes the only copy / atomic transfer | orphan cleanup, graceful forced requeue, simultaneous reapers, Redis response loss | real Redis source/destination assertions; fail after write; broken pop-first control |
| No unapproved replay / Rails policy | new/legacy Active Job payloads, native jobs, unknown classes, mailers | default interruption goes to Dead, explicitly safe opt-in requeues |
| Due work remains durable / scheduler | schedule and retry, future items, rejected client middleware | time-selected exact payload moves; injected pre/post-command failures; both sets; incompatible middleware preserves source |
| Domain identity survives / serializers | large integer IDs, nested args, Unicode, queue selection | exact round-trip payload values and stable jid |
| Operators can recover / native Dead | interruption, retry exhaustion, malformed state | Dead API lookup/manual retry; bad payload stays at source |

Tests must use isolated Redis instances, never production URLs. This contract
does not claim that a Redis server can lose its durable data without job loss.

## Executed verification (2026-09-23)

- `bundle exec parallel_rspec -n 4 --serialize-stdout spec`: 77 examples,
  zero failures, Ruby 3.4.10 / Sidekiq 6.5.12 / disposable local Redis.
- Pop-before-destination control in a separate temporary checkout: all three
  selected source-retention tests fail, detecting empty source data after a
  destination write error (working list, schedule, retry).
- Independent source review: no remaining Critical/Important findings after
  fixing shutdown requeue, invalid-batch starvation and restricting middleware.
- Local tests are not evidence of production Redis durability or throughput.

Working-source LPOS and LREM are O(worker concurrency) under normal operation;
oversized historical working lists cost O(list size) per transfer. Redis 6.0.6+
is required. Recovery reads bounded batches but drains all valid orphan work;
malformed entries are retained without blocking valid siblings.
