# frozen_string_literal: true
# Junify modifications, 2026-09-23. LGPL-3.0; see LICENSE and COPYING.

require_relative 'interrupted_set'
require_relative 'reliable_transfer'

module Sidekiq
  class BaseReliableFetch
    DEFAULT_CLEANUP_INTERVAL = 60 * 60  # 1 hour
    HEARTBEAT_INTERVAL       = 20       # seconds
    HEARTBEAT_LIFESPAN       = 60       # seconds
    HEARTBEAT_RETRY_DELAY    = 1        # seconds
    WORKING_QUEUE_PREFIX     = 'working'

    # Defines how often we try to take a lease to not flood our
    # Redis server with SET requests
    DEFAULT_LEASE_INTERVAL = 2 * 60 # seconds
    LEASE_KEY              = 'reliable-fetcher-cleanup-lock'

    # Defines the COUNT parameter that will be passed to Redis SCAN command
    SCAN_COUNT = 1000

    # How much time a job can be interrupted
    DEFAULT_MAX_RETRIES_AFTER_INTERRUPTION = 3

    # Regexes for matching working queue keys
    WORKING_QUEUE_REGEX = /#{WORKING_QUEUE_PREFIX}:(queue:.*):([^:]*:[0-9]*:[0-9a-f]*)\z/.freeze
    LEGACY_WORKING_QUEUE_REGEX = /#{WORKING_QUEUE_PREFIX}:(queue:.*):([^:]*:[0-9]*)\z/.freeze

    UnitOfWork = Struct.new(:queue, :job) do
      def acknowledge
        Sidekiq.redis { |conn| conn.lrem(Sidekiq::BaseReliableFetch.working_queue_name(queue), 1, job) }
      end

      def queue_name
        queue.sub(/.*queue:/, '')
      end

      def requeue
        Sidekiq.redis do |conn|
          ReliableTransfer.call(conn,
            source: Sidekiq::BaseReliableFetch.working_queue_name(queue), destination: queue,
            original: job, replacement: job, source_kind: 'working', destination_kind: 'list',
            queue: queue.delete_prefix('queue:'))
        end
      end
    end

    def self.setup_reliable_fetch!(config)
      config = config.options unless config.respond_to?(:[])

      fetch_strategy = if config[:semi_reliable_fetch]
                         Sidekiq::SemiReliableFetch
                       else
                         Sidekiq::ReliableFetch
                       end

      config[:fetch] = fetch_strategy.new(config)

      Sidekiq.logger.info('Junify reliable fetch activated!')

      # Set the heartbeat immediately to prevent a race condition where
      # worker_dead? returns true in another thread. `start_heartbeat_thread`
      # isn't guaranteed to have run before Sidekiq attempts to fetch jobs.
      heartbeat

      start_heartbeat_thread
    end

    def self.start_heartbeat_thread
      Thread.new do
        loop do
          begin
            heartbeat

            sleep HEARTBEAT_INTERVAL
          rescue => e
            Sidekiq.logger.error("Heartbeat thread error: #{e.message}")

            sleep HEARTBEAT_RETRY_DELAY
          end
        end
      end
    end

    def self.hostname
      Socket.gethostname
    end

    def self.process_nonce
      @@process_nonce ||= SecureRandom.hex(6)
    end

    def self.identity
      @@identity ||= "#{hostname}:#{$$}:#{process_nonce}"
    end

    def self.heartbeat
      Sidekiq.redis do |conn|
        conn.set(heartbeat_key(identity), 1, ex: HEARTBEAT_LIFESPAN)
      end

      Sidekiq.logger.debug("Heartbeat for #{identity}")
    end

    def self.worker_dead?(identity, conn)
      !conn.get(heartbeat_key(identity))
    end

    def self.heartbeat_key(identity)
      "reliable-fetcher-heartbeat-#{identity.gsub(':', '-')}"
    end

    def self.working_queue_name(queue)
      "#{WORKING_QUEUE_PREFIX}:#{queue}:#{identity}"
    end

    attr_reader :cleanup_interval, :last_try_to_take_lease_at, :lease_interval,
                :queues, :use_semi_reliable_fetch,
                :strictly_ordered_queues

    def initialize(options)
      raise ArgumentError, 'missing queue list' unless options[:queues]

      @config = options
      @cleanup_interval = options.fetch(:cleanup_interval, DEFAULT_CLEANUP_INTERVAL)
      @lease_interval = options.fetch(:lease_interval, DEFAULT_LEASE_INTERVAL)
      @last_try_to_take_lease_at = 0
      @strictly_ordered_queues = !!options[:strict]
      @queues = options[:queues].map { |q| "queue:#{q}" }
    end

    def retrieve_work
      clean_working_queues! if take_lease

      retrieve_unit_of_work
    end

    def retrieve_unit_of_work
      raise NotImplementedError,
            "#{self.class} does not implement #{__method__}"
    end

    def bulk_requeue(inprogress, _options)
      return if inprogress.empty?

      inprogress.each do |unit_of_work|
        transfer_interrupted_job(unit_of_work.job, unit_of_work.queue,
          self.class.working_queue_name(unit_of_work.queue))
      end
    rescue => e
      Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{e.message}")
    end

    private

    # Junify modification, 2026-09-23: retain the source until the destination
    # write succeeds, including if the reaper itself dies or loses its reply.
    def transfer_interrupted_job(job, queue, source, heartbeat: nil)
      msg = Sidekiq.load_json(job)
      msg['interrupted_count'] = msg['interrupted_count'].to_i + 1
      exhausted = interruption_exhausted?(msg)
      dead = exhausted && @config[:interrupted_set] == 'dead'
      destination = exhausted ? (dead ? 'dead' : 'interrupted') : queue
      now = Time.now.to_f
      if dead
        msg['error_class'] = 'Sidekiq::Interrupted'
        msg['error_message'] = 'Worker interrupted; inspect partial effects before replay'
        msg['failed_at'] = now
        msg['retry_count'] ||= 0
      end
      moved = Sidekiq.redis do |conn|
        ReliableTransfer.call(conn, source: source, destination: destination,
          original: job, replacement: Sidekiq.dump_json(msg),
          source_kind: 'working', destination_kind: exhausted ? 'zset' : 'list',
          score: now, heartbeat: heartbeat, queue: queue.delete_prefix('queue:'))
      end
      if moved == 1 && exhausted
        # Retention is deliberately separate from transfer: its failure cannot
        # remove the only copy. Match the destination's existing retention.
        max_jobs = dead ? Sidekiq::DeadSet.max_jobs : Sidekiq::InterruptedSet.max_jobs
        timeout = dead ? Sidekiq::DeadSet.timeout : Sidekiq::InterruptedSet.timeout
        Sidekiq.redis do |conn|
          conn.zremrangebyscore(destination, '-inf', now - timeout)
          conn.zremrangebyrank(destination, 0, -max_jobs - 1)
        end
        Sidekiq.logger.warn("Reliable fetch saved interrupted job #{msg['jid']} in #{destination}")
      end
      moved
    end

    def extract_queue_and_identity(key)
      # New identity format is "{hostname}:{pid}:{randomhex}
      # Old identity format is "{hostname}:{pid}"
      # Queue names may also have colons (namespaced).
      # Expressing this in a single regex is unreadable

      # Test the newer expected format first, only checking the older if necessary
      original_queue, identity = key.scan(WORKING_QUEUE_REGEX).flatten
      return original_queue, identity unless original_queue.nil? || identity.nil?

      key.scan(LEGACY_WORKING_QUEUE_REGEX).flatten
    end

    # Detect "old" jobs and requeue them because the worker they were assigned
    # to probably failed miserably.
    def clean_working_queues!
      Sidekiq.logger.info('Cleaning working queues')

      Sidekiq.redis do |conn|
        conn.scan_each(match: "#{WORKING_QUEUE_PREFIX}:queue:*", count: SCAN_COUNT) do |key|
          original_queue, identity = extract_queue_and_identity(key)

          next if original_queue.nil? || identity.nil?

          clean_working_queue!(original_queue, key, heartbeat: self.class.heartbeat_key(identity)) if self.class.worker_dead?(identity, conn)
        end
      end
    end

    def clean_working_queue!(original_queue, working_queue, heartbeat: nil)
      offset = 0
      loop do
        jobs = Sidekiq.redis { |conn| conn.lrange(working_queue, offset, offset + 99) }
        retained = 0
        jobs.each do |job|
          begin
            retained += 1 if transfer_interrupted_job(job, original_queue, working_queue, heartbeat: heartbeat) == 0
          rescue StandardError => error
            retained += 1
            Sidekiq.logger.error("Reliable fetch retained working queue #{working_queue}: #{error.class}")
          end
        end
        break if jobs.length < 100
        offset += retained
      end
    end

    def interruption_exhausted?(msg)
      maximum = if @config[:interruption_retry_limit]
                  @config[:interruption_retry_limit].call(msg)
                else
                  max_retries_after_interruption(msg['class'])
                end
      raise ArgumentError, 'interruption retry limit must be an Integer' unless maximum.is_a?(Integer)
      maximum >= 0 && msg['interrupted_count'] >= maximum
    end

    def max_retries_after_interruption(worker_class)
      max_retries_after_interruption = nil

      max_retries_after_interruption ||= begin
        Object.const_get(worker_class).get_sidekiq_options['max_retries_after_interruption']
      rescue NameError
      end

      max_retries_after_interruption ||= @config[:max_retries_after_interruption]
      max_retries_after_interruption ||= DEFAULT_MAX_RETRIES_AFTER_INTERRUPTION
      max_retries_after_interruption
    end

    def take_lease
      return unless allowed_to_take_a_lease?

      @last_try_to_take_lease_at = Time.now.to_f

      Sidekiq.redis do |conn|
        conn.set(LEASE_KEY, 1, nx: true, ex: cleanup_interval)
      end
    end

    def allowed_to_take_a_lease?
      Time.now.to_f - last_try_to_take_lease_at > lease_interval
    end
  end
end
