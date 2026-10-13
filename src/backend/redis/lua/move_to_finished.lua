-- move_to_finished.lua
--
-- KEYS[1] active
-- KEYS[2] target zset (completed or failed)
-- KEYS[3] job hash
-- KEYS[4] job lock
-- KEYS[5] events
-- KEYS[6] job-hash prefix (trailing ':')
--
-- ARGV[1] id
-- ARGV[2] token
-- ARGV[3] event (completed|failed)
-- ARGV[4] return value (json; may be empty)
-- ARGV[5] reason (may be empty)
-- ARGV[6] now (ms)
-- ARGV[7] remove policy ("keep" | "remove" | "last:N" | "for:MS")
-- ARGV[8] max stream len
-- ARGV[9] stack-trace entry for this failed attempt (may be empty)
--
-- Returns 1, or -1 (job hash gone) / -2 (lock missing) / -6 (lock held by
-- another token).

local activeKey = KEYS[1]
local targetKey = KEYS[2]
local hashKey   = KEYS[3]
local lockKey   = KEYS[4]
local eventsKey = KEYS[5]
local hashPrefix = KEYS[6]

local id       = ARGV[1]
local token    = ARGV[2]
local event    = ARGV[3]
local ret      = ARGV[4]
local reason   = ARGV[5]
local nowMs    = tonumber(ARGV[6])
local policy   = ARGV[7]
local maxStream = tonumber(ARGV[8])

-- the job hash vanished while the job was active: clean up the dangling
-- active entry and lock, and report it.
if redis.call('EXISTS', hashKey) == 0 then
  redis.call('LREM', activeKey, 0, id)
  redis.call('DEL', lockKey)
  return -1
end

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
  oxnPushStack(hashKey, ARGV[9])
end
redis.call('HDEL', hashKey, 'lockToken')

-- event payload read before retention may delete the hash
local attemptsMade = redis.call('HGET', hashKey, 'attemptsMade') or '0'

oxnRetain(targetKey, hashPrefix, id, policy, nowMs)

-- event
if event == 'completed' then
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
    'event', 'completed', 'jobId', id, 'returnvalue', ret)
else
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
    'event', 'failed', 'jobId', id, 'failedReason', reason,
    'attemptsMade', attemptsMade)
end

return 1
