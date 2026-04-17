-- move_to_delayed.lua (retry-with-backoff)
--
-- KEYS[1] active
-- KEYS[2] delayed
-- KEYS[3] job hash
-- KEYS[4] job lock
-- KEYS[5] events
-- KEYS[6] marker
--
-- ARGV[1] id
-- ARGV[2] token
-- ARGV[3] retry_at (ms)
-- ARGV[4] reason
-- ARGV[5] max stream len

local activeKey  = KEYS[1]
local delayedKey = KEYS[2]
local hashKey    = KEYS[3]
local lockKey    = KEYS[4]
local eventsKey  = KEYS[5]
local markerKey  = KEYS[6]

local id       = ARGV[1]
local token    = ARGV[2]
local retryAt  = tonumber(ARGV[3])
local reason   = ARGV[4]
local maxStream = tonumber(ARGV[5])

-- verify lock
local held = redis.call('GET', lockKey)
if held == false or held == nil then return -2 end
if held ~= token then return -6 end

redis.call('LREM', activeKey, 1, id)
redis.call('DEL', lockKey)
redis.call('HDEL', hashKey, 'lockToken', 'processedOn')
if reason and reason ~= '' then
  redis.call('HSET', hashKey, 'failedReason', reason)
end
redis.call('ZADD', delayedKey, retryAt, id)
redis.call('ZADD', markerKey, retryAt, '0')

redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
  'event', 'delayed', 'jobId', id, 'delay', retryAt)

return 1
