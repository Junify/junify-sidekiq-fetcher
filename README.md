# junify-sidekiq-fetcher

Junify-maintained fork of GitLab's Sidekiq fetcher. LGPL-3.0, including all
Junify modifications. Original authors: TEA and GitLab. See LICENSE and COPYING.
The full upstream history is preserved; the fork base is recorded in
[JUNIFY.md](JUNIFY.md), along with the behavioral contract and evidence matrix.

This release targets Sidekiq 6.5.12, Ruby 3.4 and Redis 6.0.6 or later. Newer Sidekiq major versions
require separate compatibility work. The Gem is consumed by pinned Git commit;
Junify has not published this fork to RubyGems.

```ruby
gem 'junify-sidekiq-fetcher', git: 'https://github.com/Junify/junify-sidekiq-fetcher.git', ref: '<reviewed commit>'
```

```ruby
Sidekiq.configure_server do |config|
  config[:semi_reliable_fetch] = false
  config[:max_retries_after_interruption] = 0 # save for manual inspection
  config[:interrupted_set] = 'dead'          # native Sidekiq dashboard/API
  config[:cleanup_interval] = 60
  config[:lease_interval] = 30
  Sidekiq::ReliableFetch.setup_reliable_fetch!(config)
  config[:scheduled_enq] = Sidekiq::ReliableScheduler
end
```

Configure Redis before calling setup. Every worker must load this initializer.
Crash replay is distinct from exception retry. Native job options
`max_retries_after_interruption` or the config `interruption_retry_limit` callback
can opt into a reviewed replay policy; 0 saves immediately, 2 permits one replay,
-1 is unlimited. Do not infer crash safety from `retry: true`. Applications using
Active Job must resolve their wrapped job policy explicitly before opting in.

Recovery uses compare-and-transfer Redis scripts, and moves to Dead or the
ready queue without deleting the only copy. It reads abandoned queues in batches of 100, skipping malformed entries while
recovering valid siblings. Working lists normally contain at most one item per
worker thread; membership checks and removal are O(worker concurrency), without
copying the entire list into Lua. Oversized legacy lists still require O(list size)
per transfer. Recovery latency depends on heartbeat expiry,
cleanup cadence and queue backlog. Heartbeat expiry cannot distinguish a dead
worker from a paused or partitioned live worker; duplicate effects are possible.

The scheduler retains Sidekiq client normalization, but **requires an empty
client middleware chain in the worker process** (Junify's current configuration).
Registered middleware raises `UnsupportedClientMiddleware` before invoking it or
removing any entry. Stock Sidekiq removes a scheduled item before middleware;
invoking middleware concurrently while retaining the item can lose work through
uniqueness-lock cancellation. Supporting such middleware requires a separate
ownership design. Do not add client middleware without revisiting this contract.

Due schedule/retry entries move atomically using Ruby JSON serialization, keeping
large integer arguments exact. Malformed payloads and pre-write errors remain
at source for inspection without blocking later valid entries or the other set.

This requires a single Redis instance/database, not Redis Cluster. Persistence,
failover and no-eviction are deployment responsibilities. Dead retention is
bounded, not permanent archival. Database commit plus enqueue is not made atomic.
On rollback, retain a recovery-enabled worker until private `working:queue:*`
queues are drained; do not just remove the Gem or delete those queues.

## Tests

`bundle exec parallel_rspec -n 4 --serialize-stdout spec`

Tests start disposable local Redis processes and ignore the caller's REDIS_URL.
Install `redis-server` first. Real worker interruption tests and source-preservation
fault probes are included; no test touches production queues.

## Upstream documentation (historical)

gitlab-sidekiq-fetcher
======================

## This gem is no longer updated

As we only use this library inside our Rails application, we have [vendored the gem](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/115681) directly inside that application.

This gem will no longer receive updates pushed to RubyGems.

## Introduction

`gitlab-sidekiq-fetcher` is an extension to Sidekiq that adds support for reliable
fetches from Redis.

It's based on https://github.com/TEA-ebook/sidekiq-reliable-fetch.

**IMPORTANT NOTE:** Since version `0.7.0` this gem works only with `sidekiq >= 6.1` (which introduced Fetch API breaking changes). Please use version `~> 0.5` if you use older version of the `sidekiq` .

**UPGRADE NOTE:** If upgrading from 0.7.0, strongly consider a full deployed step on 0.7.1 before 0.8.0; that fixes a bug in the queue name validation that will hit if sidekiq nodes running 0.7.0 see working queues named by 0.8.0.  See https://gitlab.com/gitlab-org/sidekiq-reliable-fetch/-/merge_requests/22

There are two strategies implemented: [Reliable fetch](http://redis.io/commands/rpoplpush#pattern-reliable-queue) using `rpoplpush` command and
semi-reliable fetch that uses regular `brpop` and `lpush` to pick the job and put it to working queue. The main benefit of "Reliable" strategy is that `rpoplpush` is atomic, eliminating a race condition in which jobs can be lost.
However, it comes at a cost because `rpoplpush` can't watch multiple lists at the same time so we need to iterate over the entire queue list which significantly increases pressure on Redis when there are more than a few queues. The "semi-reliable" strategy is much more reliable than the default Sidekiq fetcher, though. Compared to the reliable fetch strategy, it does not increase pressure on Redis significantly.

### Interruption handling

Sidekiq expects any job to report succcess or to fail. In the last case, Sidekiq puts `retry_count` counter
into the job and keeps to re-run the job until the counter reched the maximum allowed value. When the job has
not been given a chance to finish its work(to report success or fail), for example, when it was killed forcibly or when the job was requeued, after receiving TERM signal, the standard retry mechanisme does not get into the game and the job will be retried indefinatelly. This is why Reliable fetcher maintains a special counter `interrupted_count`
which is used to limit the amount of such retries. In both cases, Reliable Fetcher increments counter `interrupted_count` and rejects the job from running again when the counter exceeds `max_retries_after_interruption` times (default: 3 times).
Such a job will be put to `interrupted` queue. This queue mostly behaves as Sidekiq Dead queue so it only stores a limited amount of jobs for a limited term. Same as for Dead queue, all the limits are configurable via `interrupted_max_jobs` (default: 10_000) and `interrupted_timeout_in_seconds` (default: 3 months) Sidekiq option keys.

You can also disable special handling of interrupted jobs by setting `max_retries_after_interruption` into `-1`.
In this case, interrupted jobs will be run without any limits from Reliable Fetcher and they won't be put into Interrupted queue.


## Installation

Add the following to your `Gemfile`:

```ruby
gem 'gitlab-sidekiq-fetcher', require: 'sidekiq-reliable-fetch'
```

## Configuration

Enable reliable fetches by calling this gem from your Sidekiq configuration:

```ruby
Sidekiq.configure_server do |config|
  Sidekiq::ReliableFetch.setup_reliable_fetch!(config)

  # …
end
```

There is an additional parameter `config[:semi_reliable_fetch]` you can use to switch between two strategies:

```ruby
Sidekiq.configure_server do |config|
  config[:semi_reliable_fetch] = true # Default value is false

  Sidekiq::ReliableFetch.setup_reliable_fetch!(config)
end
```

## License

LGPL-3.0, see the LICENSE file.
