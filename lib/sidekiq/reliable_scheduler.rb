# frozen_string_literal: true
# Junify modification, 2026-09-23. LGPL-3.0; see LICENSE and COPYING.

require 'sidekiq/scheduled'
require_relative 'reliable_transfer'

module Sidekiq
  # Preserve client normalization; require an empty client middleware chain.
  class ReliableScheduler
    BATCH_SIZE = 100
    class UnsupportedClientMiddleware < StandardError; end

    class Client < Sidekiq::Client
      attr_reader :moved
      def initialize(source, original, cutoff)
        super()
        middleware do |chain|
          unless chain.empty?
            raise UnsupportedClientMiddleware, 'Atomic promotion requires an empty server client middleware chain'
          end
        end
        @source, @original, @cutoff = source, original, cutoff
      end

      private

      def raw_push(payloads)
        payload = payloads.fetch(0)
        at = payload.delete('at')
        payload['enqueued_at'] = Time.now.to_f unless at
        @moved = Sidekiq.redis do |conn|
          ReliableTransfer.call(conn, source: @source,
            destination: at ? 'schedule' : "queue:#{payload.fetch('queue')}",
            original: @original, replacement: Sidekiq.dump_json(payload),
            source_kind: 'scheduled', destination_kind: at ? 'zset' : 'list',
            score: at || Time.now.to_f, cutoff: @cutoff, queue: payload.fetch('queue'))
        end == 1
      end
    end

    def enqueue_jobs(sorted_sets = Sidekiq::Scheduled::SETS)
      cutoff = Time.now.to_f
      sorted_sets.each do |source|
        break if @done
        offset = 0
        loop do
          jobs = Sidekiq.redis { |conn| conn.zrangebyscore(source, '-inf', cutoff, limit: [offset, BATCH_SIZE]) }
          retained = 0
          jobs.each do |raw|
            break if @done
            begin
              client = Client.new(source, raw, cutoff)
              client.push(Sidekiq.load_json(raw))
              retained += 1 unless client.moved
            rescue UnsupportedClientMiddleware
              # Do not swallow configuration incompatibility or delete work.
              raise
            rescue StandardError => error
              # A lost reply may mean it already moved; either state is durable.
              Sidekiq.logger.error("Reliable scheduler failed to promote #{source}: #{error.class}")
              retained += 1
            end
          end
          break if @done || jobs.length < BATCH_SIZE
          offset += retained
        end
      end
    end

    def terminate
      @done = true
    end
  end
end
