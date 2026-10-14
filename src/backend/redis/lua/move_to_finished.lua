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
-- ARGV[10] retry (1 = an earlier call with this token may already have
--          applied: the connection dropped before its reply arrived)
--
-- Idempotent per token: a call whose lock is gone but whose token already
-- finished the job (`doneToken`) returns 1 without applying twice; on a
-- retry, so does a job already deleted by its own retention.
--
-- Returns 1, or -1 (job hash gone) / -2 (lock missing) / -6 (lock held by
-- another token) / -4 (completing a parent whose children are still
-- pending; nothing changes).
--
-- A child of a flow reports to its parent before retention runs (see
-- oxnChildDone / oxnChildFailed): its return value, or its failure.

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
  if ARGV[10] == '1' then return 1 end
  redis.call('LREM', activeKey, 0, id)
  redis.call('DEL', lockKey)
  return -1
end

-- verify lock ownership
local held = redis.call('GET', lockKey)
if held ~= token then
  if redis.call('HGET', hashKey, 'doneToken') == token then return 1 end
  if not held then return -2 end
  return -6
end
if event == 'completed' and redis.call('SCARD', hashKey .. ':dependencies') > 0 then
  return -4
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
redis.call('HSET', hashKey, 'doneToken', token)

-- event payload read before retention may delete the hash
local attemptsMade = redis.call('HGET', hashKey, 'attemptsMade') or '0'

if event == 'completed' then
  oxnChildDone(hashKey, redis.call('HGET', hashKey, 'parentKey'), ret, maxStream)
else
  oxnChildFailed(hashKey, reason, nowMs, maxStream)
end

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
