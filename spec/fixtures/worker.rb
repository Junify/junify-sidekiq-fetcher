# frozen_string_literal: true
require 'junify-sidekiq-fetcher'

# Accelerate expiry only in this isolated subprocess fixture.
Sidekiq::BaseReliableFetch.send(:remove_const, :HEARTBEAT_INTERVAL)
Sidekiq::BaseReliableFetch.const_set(:HEARTBEAT_INTERVAL, 0.1)
Sidekiq::BaseReliableFetch.send(:remove_const, :HEARTBEAT_LIFESPAN)
Sidekiq::BaseReliableFetch.const_set(:HEARTBEAT_LIFESPAN, 1)
Sidekiq.configure_server do |config|
  config.redis = { url: ENV.fetch('FETCHER_TEST_REDIS_URL') }
  config[:interruption_retry_limit] = ->(_payload) { 0 }
  config[:interrupted_set] = 'dead'
  config[:cleanup_interval] = 1
  config[:lease_interval] = 0
  Sidekiq::ReliableFetch.setup_reliable_fetch!(config)
  config[:scheduled_enq] = Sidekiq::ReliableScheduler
end

class ProcessProbeWorker
  include Sidekiq::Worker
  def perform(action)
    Sidekiq.redis { |conn| conn.set("probe:#{action}", Sidekiq::BaseReliableFetch.identity) }
    sleep 60 if action == 'started'
  end
end
