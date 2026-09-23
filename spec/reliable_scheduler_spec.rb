# frozen_string_literal: true
require 'spec_helper'
require 'junify-sidekiq-fetcher'

RSpec.describe Sidekiq::ReliableScheduler do
  let(:scheduler) { described_class.new }
  let(:payload) { { 'class' => 'Worker', 'queue' => 'orders', 'jid' => 'scheduled-id', 'args' => [9_007_199_254_740_993, '日本語'], 'retry' => 0 } }
  let(:raw) { Sidekiq.dump_json(payload) }
  before { Sidekiq.redis(&:flushdb) }

  def seed(source, value = raw, at: Time.now.to_f - 1)
    Sidekiq.redis { |conn| conn.zadd(source, at, value) }
  end

  %w[schedule retry].each do |source|
    it "moves due #{source} entries and preserves future work and exact arguments" do
      seed(source)
      future = Sidekiq.dump_json(payload.merge('jid' => 'future'))
      seed(source, future, at: Time.now.to_f + 3600)
      scheduler.enqueue_jobs
      expect(Sidekiq::Queue.new('orders').first.item).to include(payload)
      expect(Sidekiq.redis { |conn| conn.zrange(source, 0, -1) }).to eq([future])
    end

    it "keeps #{source} intact on a failed destination write and succeeds later" do
      seed(source)
      Sidekiq.redis { |conn| conn.set('queue:orders', 'wrong-type') }
      scheduler.enqueue_jobs
      expect(Sidekiq.redis { |conn| conn.zrange(source, 0, -1) }).to eq([raw])
      Sidekiq.redis { |conn| conn.del('queue:orders') }
      scheduler.enqueue_jobs
      expect(Sidekiq::Queue.new('orders').map(&:jid)).to eq([payload['jid']])
    end
  end

  it 'preserves source on failed transfer before apply' do
    seed('retry')
    allow(Sidekiq::ReliableTransfer).to receive(:call).and_raise(Redis::CannotConnectError)
    scheduler.enqueue_jobs
    expect(Sidekiq.redis { |conn| conn.zrange('retry', 0, -1) }).to eq([raw])
    expect(Sidekiq::Queue.new('orders').size).to eq(0)
  end

  it 'does not duplicate a promotion after a lost successful response' do
    seed('schedule')
    allow(Sidekiq::ReliableTransfer).to receive(:call).and_wrap_original do |method, *args, **kwargs|
      method.call(*args, **kwargs)
      raise Redis::TimeoutError
    end
    scheduler.enqueue_jobs
    allow(Sidekiq::ReliableTransfer).to receive(:call).and_call_original
    scheduler.enqueue_jobs
    expect(Sidekiq::Queue.new('orders').map(&:jid)).to eq([payload['jid']])
    expect(Sidekiq::ScheduledSet.new.size).to eq(0)
  end

  it 'allows concurrent schedulers without duplicate delivery' do
    seed('schedule')
    4.times.map { Thread.new { described_class.new.enqueue_jobs } }.each(&:value)
    expect(Sidekiq::Queue.new('orders').map(&:jid)).to eq([payload['jid']])
  end

  it 'drains more than one bounded batch instead of limiting total throughput' do
    205.times { |i| seed('schedule', Sidekiq.dump_json(payload.merge('jid' => "job-#{i}"))) }
    scheduler.enqueue_jobs
    expect(Sidekiq::Queue.new('orders').map(&:jid).sort).to eq(205.times.map { |i| "job-#{i}" }.sort)
  end

  it 'keeps malformed entries without preventing later valid work in either set' do
    seed('schedule', '{broken')
    seed('schedule')
    seed('retry', Sidekiq.dump_json(payload.merge('jid' => 'retry-id')))
    scheduler.enqueue_jobs
    expect(Sidekiq::Queue.new('orders').map(&:jid).sort).to eq(%w[retry-id scheduled-id])
    expect(Sidekiq.redis { |conn| conn.zrange('schedule', 0, -1) }).to eq(['{broken'])
  end

  %w[schedule retry].each do |source|
    it "progresses beyond a full retained batch in #{source}" do
      105.times { |i| seed(source, "{broken-#{i}", at: Time.now.to_f - 100) }
      seed(source)
      scheduler.enqueue_jobs
      expect(Sidekiq::Queue.new('orders').map(&:jid)).to eq([payload['jid']])
      expect(Sidekiq.redis { |conn| conn.zcard(source) }).to eq(105)
    end
  end

  class CancellationMiddleware
    def call(*)
      raise 'Unsupported middleware must never execute'
    end
  end

  %w[schedule retry].each do |source|
    it "fails closed before invoking client middleware for #{source}" do
      seed(source)
      Sidekiq.client_middleware { |chain| chain.add(CancellationMiddleware) }
      expect { scheduler.enqueue_jobs }.to raise_error(described_class::UnsupportedClientMiddleware)
      expect(Sidekiq.redis { |conn| conn.zrange(source, 0, -1) }).to eq([raw])
      expect(Sidekiq::Queue.new('orders').size).to eq(0)
      Sidekiq.client_middleware { |chain| chain.remove(CancellationMiddleware) }
      scheduler.enqueue_jobs
      expect(Sidekiq::Queue.new('orders').map(&:jid)).to eq([payload['jid']])
    ensure
      Sidekiq.client_middleware { |chain| chain.remove(CancellationMiddleware) }
    end
  end
end
