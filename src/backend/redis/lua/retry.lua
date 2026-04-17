-- retry.lua — move a failed job back to wait, clearing failure state.
--
-- KEYS[1] failed zset
-- KEYS[2] wait list
-- KEYS[3] job hash
-- KEYS[4] events
-- KEYS[5] marker
-- ARGV[1] id
-- ARGV[2] max stream len

local failedKey = KEYS[1]
local waitKey   = KEYS[2]
local hashKey   = KEYS[3]
local eventsKey = KEYS[4]
local markerKey = KEYS[5]

local id        = ARGV[1]
local maxStream = tonumber(ARGV[2])

local score = redis.call('ZSCORE', failedKey, id)
if not score then return -3 end
redis.call('ZREM', failedKey, id)
redis.call('HDEL', hashKey, 'failedReason', 'finishedOn', 'stacktrace')
redis.call('HSET', hashKey, 'attemptsMade', '0')
redis.call('LPUSH', waitKey, id)
redis.call('ZADD', markerKey, 0, '0')

redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
  'event', 'waiting', 'jobId', id)
return 1
