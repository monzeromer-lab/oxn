-- remove.lua — delete a job from every queue-level container and drop its keys.
--
-- Returns 1, or -1 (no such job) / -7 (active and locked).
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

-- -1: nothing to remove. -7: the job is active and a worker holds its
-- lock — deleting it would pull the job out from under that worker.
if redis.call('EXISTS', KEYS[8]) == 0 then return -1 end
if redis.call('EXISTS', KEYS[9]) == 1 and redis.call('LPOS', KEYS[3], id) then
  return -7
end

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
