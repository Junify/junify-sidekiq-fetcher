# frozen_string_literal: true
require 'spec_helper'
require 'junify-sidekiq-fetcher'
require 'rbconfig'

RSpec.describe 'Real Sidekiq worker lifecycle' do
  def eventually
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
    loop do
      return if yield
      raise 'Worker lifecycle condition timed out' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
      sleep 0.05
    end
  end

  def start_worker
    Process.spawn({ 'FETCHER_TEST_REDIS_URL' => REDIS_URL },
      RbConfig.ruby, '-Ilib', Gem.bin_path('sidekiq', 'sidekiq'),
      '-r', File.expand_path('fixtures/worker.rb', __dir__), '-q', 'probe', '-c', '1', '-t', '1',
      out: @log.path, err: [:child, :out])
  end

  it 'retains a SIGKILLed execution, saves it without replay, and acknowledges subsequent success' do
    require 'tempfile'
    @log = Tempfile.new('junify-sidekiq-worker-')
    Sidekiq.redis(&:flushdb)
    jid = Sidekiq::Client.push('class' => 'ProcessProbeWorker', 'queue' => 'probe', 'args' => ['started'])
    first_pid = start_worker
    eventually { Sidekiq.redis { |conn| conn.get('probe:started') } }
    identity = Sidekiq.redis { |conn| conn.get('probe:started') }
    source = "working:queue:probe:#{identity}"
    expect(Sidekiq.redis { |conn| conn.lrange(source, 0, -1) }.map { |raw| Sidekiq.load_json(raw)['jid'] }).to eq([jid])
    Process.kill('KILL', first_pid)
    Process.wait(first_pid)
    first_pid = nil
    # A real fresh process must discover the abandoned list after heartbeat expiry.
    second_pid = start_worker
    Sidekiq::Client.push('class' => 'ProcessProbeWorker', 'queue' => 'probe', 'args' => ['completed'])
    eventually { Sidekiq::DeadSet.new.find_job(jid) && Sidekiq.redis { |conn| conn.get('probe:completed') } }
    expect(Sidekiq::DeadSet.new.find_job(jid).item['error_class']).to eq('Sidekiq::Interrupted')
    expect(Sidekiq.redis { |conn| conn.get('probe:started') }).to eq(identity)
    eventually { Sidekiq.redis { |conn| conn.scan_each(match: 'working:queue:probe:*').to_a.empty? } }
    expect(Sidekiq::Queue.new('probe').size).to eq(0)
  rescue Exception
    warn File.read(@log.path) if @log
    raise
  ensure
    [first_pid, second_pid].compact.each do |pid|
      Process.kill('KILL', pid) rescue Errno::ESRCH
      Process.wait(pid) rescue Errno::ECHILD
    end
    @log&.close!
  end
end
