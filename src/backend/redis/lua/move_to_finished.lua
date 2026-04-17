-- move_to_finished.lua
--
-- KEYS[1] active
-- KEYS[2] target zset (completed or failed)
-- KEYS[3] job hash
-- KEYS[4] job lock
-- KEYS[5] events
--
-- ARGV[1] id
-- ARGV[2] token
-- ARGV[3] event (completed|failed)
-- ARGV[4] return value (json; may be empty)
-- ARGV[5] reason (may be empty)
-- ARGV[6] now (ms)
-- ARGV[7] remove policy ("keep" | "remove" | "last:N" | "for:MS")
-- ARGV[8] max stream len

local activeKey = KEYS[1]
local targetKey = KEYS[2]
local hashKey   = KEYS[3]
local lockKey   = KEYS[4]
local eventsKey = KEYS[5]

local id       = ARGV[1]
local token    = ARGV[2]
local event    = ARGV[3]
local ret      = ARGV[4]
local reason   = ARGV[5]
local nowMs    = tonumber(ARGV[6])
local policy   = ARGV[7]
local maxStream = tonumber(ARGV[8])

-- verify lock ownership
local held = redis.call('GET', lockKey)
if held == false or held == nil then
  return -2
end
if held ~= token then
  return -6
end

-- remove from active and delete lock
redis.call('LREM', activeKey, 1, id)
redis.call('DEL', lockKey)

-- persist outcome on the job hash
redis.call('HSET', hashKey,
  'finishedOn', nowMs)
if event == 'completed' then
  if ret and ret ~= '' then
    redis.call('HSET', hashKey, 'returnvalue', ret)
  end
else
  if reason and reason ~= '' then
    redis.call('HSET', hashKey, 'failedReason', reason)
  end
end
redis.call('HDEL', hashKey, 'lockToken')

-- retention policy
if policy == 'remove' then
  redis.call('DEL', hashKey)
else
  redis.call('ZADD', targetKey, nowMs, id)
  if string.sub(policy, 1, 5) == 'last:' then
    local n = tonumber(string.sub(policy, 6))
    if n and n > 0 then
      local total = redis.call('ZCARD', targetKey)
      if total > n then
        redis.call('ZREMRANGEBYRANK', targetKey, 0, total - n - 1)
      end
    end
  elseif string.sub(policy, 1, 4) == 'for:' then
    local ms = tonumber(string.sub(policy, 5))
    if ms and ms > 0 then
      redis.call('ZREMRANGEBYSCORE', targetKey, 0, nowMs - ms)
    end
  end
end

-- event
if event == 'completed' then
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
    'event', 'completed', 'jobId', id, 'returnvalue', ret)
else
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
    'event', 'failed', 'jobId', id, 'failedReason', reason,
    'attemptsMade', redis.call('HGET', hashKey, 'attemptsMade') or '0')
end

return 1
