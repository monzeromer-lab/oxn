-- scan_stalled.lua
--
-- Detect jobs in `active` whose lock has expired. Re-queue them if they
-- haven't exceeded max_stalled; otherwise fail them permanently.
--
-- KEYS[1] active
-- KEYS[2] wait
-- KEYS[3] stalled (set)
-- KEYS[4] stalled_check (rate-limit flag)
-- KEYS[5] events
-- KEYS[6] job-hash prefix
--
-- ARGV[1] now (ms)
-- ARGV[2] max stalled
-- ARGV[3] max stream len

local activeKey   = KEYS[1]
local waitKey     = KEYS[2]
local stalledKey  = KEYS[3]
local flagKey     = KEYS[4]
local eventsKey   = KEYS[5]
local hashPrefix  = KEYS[6]

local nowMs     = tonumber(ARGV[1])
local maxStalled = tonumber(ARGV[2])
local maxStream  = tonumber(ARGV[3])

-- run at most once per second globally; exit early otherwise
local acquired = redis.call('SET', flagKey, nowMs, 'NX', 'PX', 1000)
if not acquired then
  return {}
end

local moved = {}
local active = redis.call('LRANGE', activeKey, 0, -1)
for i = 1, #active do
  local id = active[i]
  local hashKey = hashPrefix .. id
  local lockKey = hashKey .. ':lock'
  local has = redis.call('EXISTS', lockKey)
  if has == 0 then
    -- track how many times this job stalled
    local stalled = tonumber(redis.call('HINCRBY', hashKey, 'stalledCounter', 1))
    redis.call('LREM', activeKey, 1, id)
    if stalled > maxStalled then
      redis.call('HSET', hashKey,
        'failedReason', 'job stalled more than allowable limit',
        'finishedOn', nowMs)
      redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
        'event', 'failed', 'jobId', id, 'failedReason', 'stalled')
    else
      redis.call('SADD', stalledKey, id)
      redis.call('LPUSH', waitKey, id)
      redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
        'event', 'stalled', 'jobId', id)
      table.insert(moved, id)
    end
  end
end
return moved
