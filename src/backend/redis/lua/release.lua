-- release.lua — hand an active job back without recording an outcome.
--
-- Used when a worker shuts down and its handler did not finish within
-- shutdown_timeout. The aborted attempt is refunded (attemptsMade - 1), the
-- lock is dropped and the job goes back to the head of its ready container
-- (prioritized / wait / paused).
--
-- KEYS[1] active
-- KEYS[2] wait
-- KEYS[3] paused
-- KEYS[4] prioritized
-- KEYS[5] priority counter
-- KEYS[6] meta
-- KEYS[7] marker
-- KEYS[8] events
-- KEYS[9] job hash
-- KEYS[10] job lock
--
-- ARGV[1] id
-- ARGV[2] token
-- ARGV[3] max stream len
-- ARGV[4] retry (idempotent per token, see move_to_finished.lua)
--
-- Returns 1, or -1 (job gone) / -2 (lock missing) / -6 (lock held by
-- another token).

local id        = ARGV[1]
local token     = ARGV[2]
local maxStream = tonumber(ARGV[3])
local hashKey   = KEYS[9]
local lockKey   = KEYS[10]

if redis.call('EXISTS', hashKey) == 0 then
  if ARGV[4] == '1' then return 1 end
  return -1
end
local held = redis.call('GET', lockKey)
if held ~= token then
  if redis.call('HGET', hashKey, 'doneToken') == token then return 1 end
  if not held then return -2 end
  return -6
end

redis.call('LREM', KEYS[1], 1, id)
redis.call('DEL', lockKey)
redis.call('HDEL', hashKey, 'lockToken', 'processedOn')
redis.call('HSET', hashKey, 'doneToken', token)
if tonumber(redis.call('HGET', hashKey, 'attemptsMade') or '0') > 0 then
  redis.call('HINCRBY', hashKey, 'attemptsMade', -1)
end

local landed = oxnPushReady({
  wait = KEYS[2], paused = KEYS[3], prio = KEYS[4], pc = KEYS[5],
  meta = KEYS[6], marker = KEYS[7],
}, id, hashKey, true)

oxnEmit(KEYS[8], maxStream, 'event', 'waiting', 'jobId', id, 'prev', 'active', 'released', landed)
return 1
