# frozen_string_literal: true
# Junify modification, 2026-09-23. LGPL-3.0; see LICENSE and COPYING.

module Sidekiq
  # Never remove a source payload until Redis has accepted its destination.
  # Keep JSON processing in Ruby to preserve integer arguments above 2**53.
  module ReliableTransfer
    SCRIPT = <<~LUA.freeze
      local src, dst, heartbeat = KEYS[1], KEYS[2], KEYS[3]
      local original, replacement, source_kind, dest_kind = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
      local score, cutoff = tonumber(ARGV[5]), tonumber(ARGV[6])
      if ARGV[7] == '1' and redis.call('EXISTS', heartbeat) == 1 then return 0 end
      if source_kind == 'scheduled' then
        local current = redis.call('ZSCORE', src, original)
        if not current or tonumber(current) > cutoff then return 0 end
      else
        if not redis.call('LPOS', src, original) then return 0 end
      end
      if dest_kind == 'list' then
        -- Register first: a corrupt queues key must not produce a half-transfer.
        redis.call('SADD', KEYS[4], ARGV[8])
        redis.call('LPUSH', dst, replacement)
      else
        redis.call('ZADD', dst, score, replacement)
      end
      if source_kind == 'scheduled' then
        if src ~= dst or original ~= replacement then redis.call('ZREM', src, original) end
      else
        redis.call('LREM', src, -1, original)
      end
      return 1
    LUA

    def self.call(connection, source:, destination:, original:, replacement:,
                  source_kind:, destination_kind:, score: Time.now.to_f,
                  cutoff: Time.now.to_f, heartbeat: nil, queue: '')
      connection.eval(SCRIPT,
        keys: [source, destination, heartbeat || source, 'queues'],
        argv: [original, replacement, source_kind, destination_kind, score, cutoff, heartbeat ? '1' : '0', queue])
    end
  end
end
