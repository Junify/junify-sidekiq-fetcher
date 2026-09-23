# frozen_string_literal: true
require 'spec_helper'
require 'junify-sidekiq-fetcher'

RSpec.describe 'Durable interruption recovery' do
  let(:identity) { 'terminated:123:abc123' }
  let(:source) { "working:queue:orders:#{identity}" }
  let(:heartbeat) { Sidekiq::BaseReliableFetch.heartbeat_key(identity) }
  let(:payload) { { 'class' => 'MissingWorker', 'queue' => 'orders', 'jid' => 'original-jid', 'args' => [9_007_199_254_740_993, { 'message' => '日本語' }] } }
  let(:raw) { Sidekiq.dump_json(payload) }
  let(:options) { { queues: ['orders'], interruption_retry_limit: ->(_job) { 0 }, interrupted_set: 'dead' } }
  let(:fetcher) { Sidekiq::ReliableFetch.new(options) }

  before { Sidekiq.redis(&:flushdb) }

  def recover
    fetcher.send(:clean_working_queue!, 'queue:orders', source, heartbeat: heartbeat)
  end

  def seed
    Sidekiq.redis { |conn| conn.lpush(source, raw) }
  end

  def dead
    Sidekiq::DeadSet.new.find_job(payload['jid'])
  end

  it 'saves an interruption without replay, preserving identity and large IDs' do
    seed
    recover
    expect(dead.args).to eq(payload['args'])
    expect(dead.item).to include('error_class' => 'Sidekiq::Interrupted', 'interrupted_count' => 1)
    expect(Sidekiq.redis { |conn| conn.llen(source) }).to eq(0)
    expect(Sidekiq::Queue.new('orders').size).to eq(0)
    # Native operator retry remains usable after inspecting partial side effects.
    dead.retry
    expect(Sidekiq::Queue.new('orders').first.args).to eq(payload['args'])
  end

  it 'requeues only the explicit crash-retry policy and eventually saves it' do
    options[:interruption_retry_limit] = ->(_job) { 2 }
    seed
    recover
    recovered = Sidekiq.redis { |conn| conn.rpop('queue:orders') }
    expect(Sidekiq.load_json(recovered)['interrupted_count']).to eq(1)
    Sidekiq.redis { |conn| conn.lpush(source, recovered) }
    recover
    expect(dead.item['interrupted_count']).to eq(2)
  end

  it 'retains the original when the destination write fails, then recovers later' do
    seed
    Sidekiq.redis { |conn| conn.set('dead', 'wrong-type') }
    recover
    expect(Sidekiq.redis { |conn| conn.lrange(source, 0, -1) }).to eq([raw])
    Sidekiq.redis { |conn| conn.del('dead') }
    recover
    expect(dead).not_to be_nil
  end

  it 'survives loss of the successful transfer response without duplicating it' do
    seed
    allow(Sidekiq::ReliableTransfer).to receive(:call).and_wrap_original do |method, *args, **kwargs|
      method.call(*args, **kwargs)
      raise Redis::TimeoutError, 'response lost after apply'
    end
    recover
    expect(dead).not_to be_nil
    allow(Sidekiq::ReliableTransfer).to receive(:call).and_call_original
    recover
    expect(Sidekiq::DeadSet.new.size).to eq(1)
    expect(Sidekiq.redis { |conn| conn.llen(source) }).to eq(0)
  end

  it 'does not reclaim a live worker, including a heartbeat refreshed just before transfer' do
    seed
    allow(Sidekiq::ReliableTransfer).to receive(:call).and_wrap_original do |method, *args, **kwargs|
      Sidekiq.redis { |conn| conn.set(heartbeat, 'alive', ex: 60) }
      method.call(*args, **kwargs)
    end
    recover
    expect(Sidekiq.redis { |conn| conn.lrange(source, 0, -1) }).to eq([raw])
    expect(dead).to be_nil
  end

  it 'allows two reapers to race without duplicating ready work' do
    options[:interruption_retry_limit] = ->(_job) { -1 }
    seed
    threads = 4.times.map { Thread.new { recover } }
    threads.each(&:value)
    expect(Sidekiq::Queue.new('orders').map(&:jid)).to eq([payload['jid']])
    expect(Sidekiq.redis { |conn| conn.llen(source) }).to eq(0)
  end

  it 'keeps malformed JSON in the working queue for inspection' do
    Sidekiq.redis { |conn| conn.lpush(source, '{broken') }
    recover
    expect(Sidekiq.redis { |conn| conn.lrange(source, 0, -1) }).to eq(['{broken'])
  end

  it 'uses the same no-replay policy for forced graceful shutdown' do
    own_source = Sidekiq::BaseReliableFetch.working_queue_name('queue:orders')
    Sidekiq.redis { |conn| conn.lpush(own_source, raw) }
    unit = Sidekiq::BaseReliableFetch::UnitOfWork.new('queue:orders', raw)
    fetcher.bulk_requeue([unit], nil)
    expect(dead).not_to be_nil
    expect(Sidekiq.redis { |conn| conn.llen(own_source) }).to eq(0)
  end

  it 'recovers valid siblings behind a full batch of malformed working entries' do
    seed
    105.times { |i| Sidekiq.redis { |conn| conn.lpush(source, "{broken-#{i}") } }
    recover
    expect(dead).not_to be_nil
    expect(Sidekiq.redis { |conn| conn.llen(source) }).to eq(105)
    recover
    expect(Sidekiq::DeadSet.new.size).to eq(1)
  end

  it 'preserves a fetched but unstarted job when shutdown requeue fails' do
    own_source = Sidekiq::BaseReliableFetch.working_queue_name('queue:orders')
    Sidekiq.redis { |conn| conn.lpush(own_source, raw); conn.set('queue:orders', 'wrong-type') }
    unit = Sidekiq::BaseReliableFetch::UnitOfWork.new('queue:orders', raw)
    expect { unit.requeue }.to raise_error(Redis::CommandError)
    expect(Sidekiq.redis { |conn| conn.lrange(own_source, 0, -1) }).to eq([raw])
    Sidekiq.redis { |conn| conn.del('queue:orders') }
    # A successful command with a lost response must also be safe to retry.
    allow(Sidekiq::ReliableTransfer).to receive(:call).and_wrap_original do |method, *args, **kwargs|
      method.call(*args, **kwargs)
      raise Redis::TimeoutError
    end
    expect { unit.requeue }.to raise_error(Redis::TimeoutError)
    allow(Sidekiq::ReliableTransfer).to receive(:call).and_call_original
    unit.requeue
    expect(Sidekiq.redis { |conn| conn.lrange('queue:orders', 0, -1) }).to eq([raw])
    expect(Sidekiq.redis { |conn| conn.llen(own_source) }).to eq(0)
  end

  it 'preserves the interrupted job when the recovering process is SIGKILLed before transfer' do
    seed
    reader, writer = IO.pipe
    pid = fork do
      reader.close
      fetcher.define_singleton_method(:transfer_interrupted_job) do |*args, **kwargs|
        writer.write('ready'); writer.flush
        sleep 60
        super(*args, **kwargs)
      end
      recover
      exit! 0
    end
    writer.close
    expect(IO.select([reader], nil, nil, 5)).not_to be_nil
    expect(reader.read(5)).to eq('ready')
    Process.kill('KILL', pid)
    Process.wait(pid)
    pid = nil
    expect(Sidekiq.redis { |conn| conn.lrange(source, 0, -1) }).to eq([raw])
    recover
    expect(dead).not_to be_nil
  ensure
    Process.kill('KILL', pid) rescue Errno::ESRCH if pid
    Process.wait(pid) rescue Errno::ECHILD if pid
    reader&.close
    writer&.close unless writer&.closed?
  end
end
