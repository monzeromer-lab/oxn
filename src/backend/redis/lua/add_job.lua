-- add_job.lua
--
-- KEYS[1]  meta hash
-- KEYS[2]  id counter
-- KEYS[3]  wait list
-- KEYS[4]  paused list
-- KEYS[5]  delayed zset
-- KEYS[6]  prioritized zset
-- KEYS[7]  priority counter
-- KEYS[8]  events stream
-- KEYS[9]  marker key
-- KEYS[10] dedup key (may be empty string)
-- KEYS[11] job-hash prefix (includes trailing ':')
--
-- ARGV[1]  name
-- ARGV[2]  data (JSON)
-- ARGV[3]  opts (JSON)
-- ARGV[4]  timestamp (ms)
-- ARGV[5]  delay (ms)
-- ARGV[6]  priority (int)
-- ARGV[7]  dedup id (may be empty string)
-- ARGV[8]  dedup TTL (ms)
-- ARGV[9]  explicit id (may be empty)
-- ARGV[10] lifo (0|1)
-- ARGV[11] max stream len

local metaKey       = KEYS[1]
local idCounterKey  = KEYS[2]
local waitKey       = KEYS[3]
local pausedKey     = KEYS[4]
local delayedKey    = KEYS[5]
local prioKey       = KEYS[6]
local prioCounter   = KEYS[7]
local eventsKey     = KEYS[8]
local markerKey     = KEYS[9]
local dedupKey      = KEYS[10]
local hashPrefix    = KEYS[11]

local name      = ARGV[1]
local data      = ARGV[2]
local optsJson  = ARGV[3]
local timestamp = tonumber(ARGV[4])
local delayMs   = tonumber(ARGV[5])
local priority  = tonumber(ARGV[6])
local dedupId   = ARGV[7]
local dedupTtl  = tonumber(ARGV[8])
local explicit  = ARGV[9]
local lifo      = tonumber(ARGV[10])
local maxStream = tonumber(ARGV[11])

-- deduplication: if the key exists, emit deduplicated and return existing id
if dedupId ~= '' and dedupKey ~= '' then
  local existing = redis.call('GET', dedupKey)
  if existing then
    redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
      'event', 'deduplicated', 'jobId', existing, 'dedupId', dedupId)
    return existing
  end
end

-- compute id
local id
if explicit ~= '' then
  id = explicit
else
  id = tostring(redis.call('INCR', idCounterKey))
end

local hashKey = hashPrefix .. id

-- write the job hash
redis.call('HSET', hashKey,
  'name', name,
  'data', data,
  'opts', optsJson,
  'timestamp', timestamp,
  'attemptsMade', 0)

-- set dedup pointer
if dedupId ~= '' and dedupKey ~= '' and dedupTtl > 0 then
  redis.call('SET', dedupKey, id, 'PX', dedupTtl)
end

local queueState
if delayMs > 0 then
  local score = timestamp + delayMs
  redis.call('ZADD', delayedKey, score, id)
  -- wake any worker blocked on marker
  redis.call('ZADD', markerKey, score, '0')
  queueState = 'delayed'
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
    'event', 'delayed', 'jobId', id, 'delay', score)
elseif priority > 0 then
  local counter = redis.call('INCR', prioCounter)
  -- pack: higher priority bits (inverted) then counter for FIFO within tier
  local score = priority * 0x100000000 + counter
  redis.call('ZADD', prioKey, score, id)
  queueState = 'prioritized'
else
  -- choose wait or paused list based on meta.paused
  local isPaused = redis.call('HGET', metaKey, 'paused')
  local target = waitKey
  if isPaused == '1' then
    target = pausedKey
  end
  if lifo == 1 then
    redis.call('RPUSH', target, id)
  else
    redis.call('LPUSH', target, id)
  end
  queueState = 'waiting'
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
    'event', 'waiting', 'jobId', id)
end

redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
  'event', 'added', 'jobId', id, 'name', name)

return id
