-- promote.lua — move a delayed job to wait ahead of schedule.
--
-- KEYS[1] delayed zset
-- KEYS[2] wait list
-- KEYS[3] events
-- KEYS[4] marker
-- KEYS[5] job hash
-- ARGV[1] id
-- ARGV[2] max stream len

local delayedKey = KEYS[1]
local waitKey    = KEYS[2]
local eventsKey  = KEYS[3]
local markerKey  = KEYS[4]
local hashKey    = KEYS[5]

local id        = ARGV[1]
local maxStream = tonumber(ARGV[2])

-- -1: no such job; -3: the job exists but is not delayed.
if redis.call('EXISTS', hashKey) == 0 then return -1 end
local score = redis.call('ZSCORE', delayedKey, id)
if not score then return -3 end
redis.call('ZREM', delayedKey, id)
redis.call('LPUSH', waitKey, id)
oxnWake(markerKey)

redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
  'event', 'waiting', 'jobId', id)
return 1
