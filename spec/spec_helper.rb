# frozen_string_literal: true
require 'sidekiq'
require 'sidekiq/api'
require 'stub_env'
require 'socket'
require 'tmpdir'
require 'fileutils'

# Every parallel worker owns a disposable Redis. Never connect to a caller's
# REDIS_URL: the upstream examples use FLUSHDB.
redis_directory = Dir.mktmpdir('junify-fetcher-spec-')
socket = TCPServer.new('127.0.0.1', 0)
port = socket.addr[1]
socket.close
redis_pid = Process.spawn('redis-server', '--bind', '127.0.0.1', '--port', port.to_s,
  '--save', '', '--appendonly', 'no', '--dir', redis_directory,
  out: File.join(redis_directory, 'redis.log'), err: [:child, :out])
at_exit do
  Process.kill('TERM', redis_pid) rescue Errno::ESRCH
  Process.wait(redis_pid) rescue Errno::ECHILD
  FileUtils.remove_entry(redis_directory)
end
REDIS_URL = "redis://127.0.0.1:#{port}/0"
Sidekiq.configure_client { |config| config.redis = { url: REDIS_URL } }
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
loop do
  begin
    break if Sidekiq.redis(&:ping) == 'PONG'
  rescue Redis::CannotConnectError
    raise if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
    sleep 0.02
  end
end
Sidekiq.logger.level = Logger::ERROR
RSpec.configure do |config|
  config.include StubEnv::Helpers
  config.mock_with(:rspec) { |mocks| mocks.verify_partial_doubles = true }
end
