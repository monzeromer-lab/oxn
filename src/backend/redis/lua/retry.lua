-- retry.lua — move a failed job back to wait, clearing failure state.
--
-- KEYS[1] failed zset
-- KEYS[2] wait list
-- KEYS[3] job hash
-- KEYS[4] events
-- KEYS[5] marker
-- KEYS[6] paused
-- KEYS[7] prioritized
-- KEYS[8] priority counter
-- KEYS[9] meta
-- KEYS[10] waiting-children
-- ARGV[1] id
--
-- A parent whose children are still pending goes back to waiting for them
-- (`waiting-children`) instead of becoming ready.
-- ARGV[2] max stream len

local failedKey = KEYS[1]
local waitKey   = KEYS[2]
local hashKey   = KEYS[3]
local eventsKey = KEYS[4]
local markerKey = KEYS[5]

local id        = ARGV[1]
local maxStream = tonumber(ARGV[2])

-- -1: no such job; -3: the job exists but is not in `failed`.
if redis.call('EXISTS', hashKey) == 0 then return -1 end
local score = redis.call('ZSCORE', failedKey, id)
if not score then return -3 end
redis.call('ZREM', failedKey, id)
redis.call('HDEL', hashKey, 'failedReason', 'finishedOn', 'stacktrace')
redis.call('HSET', hashKey, 'attemptsMade', '0')
if redis.call('SCARD', hashKey .. ':dependencies') > 0 then
  redis.call('ZADD', KEYS[10], oxnNowMs(), id)
  oxnEmit(eventsKey, maxStream, 'event', 'waiting-children', 'jobId', id, 'prev', 'failed')
  return 1
end
local landed = oxnPushReady({
  wait = waitKey, paused = KEYS[6], prio = KEYS[7], pc = KEYS[8],
  meta = KEYS[9], marker = markerKey,
}, id, hashKey, false)
oxnEmit(eventsKey, maxStream, 'event', 'waiting', 'jobId', id, 'prev', 'failed', 'landed', landed)
return 1
