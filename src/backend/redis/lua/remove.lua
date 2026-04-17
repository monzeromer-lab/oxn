-- remove.lua — delete a job from every queue-level container and drop its keys.
--
-- KEYS[1] wait
-- KEYS[2] paused
-- KEYS[3] active
-- KEYS[4] delayed
-- KEYS[5] prioritized
-- KEYS[6] completed
-- KEYS[7] failed
-- KEYS[8] job hash
-- KEYS[9] job lock
-- KEYS[10] job logs
-- KEYS[11] events
-- ARGV[1] id
-- ARGV[2] max stream len

local id        = ARGV[1]
local maxStream = tonumber(ARGV[2])

redis.call('LREM', KEYS[1], 0, id)
redis.call('LREM', KEYS[2], 0, id)
redis.call('LREM', KEYS[3], 0, id)
redis.call('ZREM', KEYS[4], id)
redis.call('ZREM', KEYS[5], id)
redis.call('ZREM', KEYS[6], id)
redis.call('ZREM', KEYS[7], id)
redis.call('DEL', KEYS[8])
redis.call('DEL', KEYS[9])
redis.call('DEL', KEYS[10])

redis.call('XADD', KEYS[11], 'MAXLEN', '~', maxStream, '*',
  'event', 'removed', 'jobId', id)
return 1
