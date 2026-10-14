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
-- ARGV[6] postpone (1 = Error::Delayed / RateLimited: refund the attempt
--         and record no failure; 0 = retry backoff after a real failure)
-- ARGV[7] stack-trace entry for this failed attempt (may be empty)
-- ARGV[8] retry (1 = an earlier call with this token may already have
--         applied; idempotent per token, see move_to_finished.lua)
--
-- Returns 1, or -2 (lock missing) / -6 (lock held by another token).

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
local postpone = ARGV[6] == '1'

if ARGV[8] == '1' and redis.call('EXISTS', hashKey) == 0 then return 1 end
-- verify lock
local held = redis.call('GET', lockKey)
if held ~= token then
  if redis.call('HGET', hashKey, 'doneToken') == token then return 1 end
  if not held then return -2 end
  return -6
end

redis.call('LREM', activeKey, 1, id)
redis.call('DEL', lockKey)
redis.call('HDEL', hashKey, 'lockToken', 'processedOn')
redis.call('HSET', hashKey, 'doneToken', token)
if postpone then
  -- not a failure: the handler asked to run later
  if tonumber(redis.call('HGET', hashKey, 'attemptsMade') or '0') > 0 then
    redis.call('HINCRBY', hashKey, 'attemptsMade', -1)
  end
else
  if reason and reason ~= '' then
    redis.call('HSET', hashKey, 'failedReason', reason)
  end
  oxnPushStack(hashKey, ARGV[7])
end
redis.call('ZADD', delayedKey, retryAt, id)
-- Wake a blocked worker so it re-computes its block time against this
-- job's due time.
oxnWake(markerKey)

redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
  'event', 'delayed', 'jobId', id, 'delay', retryAt, 'postponed', postpone and '1' or '0')

return 1
