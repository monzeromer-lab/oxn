-- move_to_waiting_children.lua — the handler returned
-- `Error::WaitingChildren`: park the active job until its children
-- resolve.
--
-- The attempt is refunded (waiting is not a failure). With pending
-- children the job moves to `waiting-children` and becomes ready again
-- when the last one resolves; with none left it is ready again at once.
--
-- KEYS[1] active
-- KEYS[2] job hash
-- KEYS[3] job lock
-- KEYS[4] waiting-children
-- ARGV[1] id
-- ARGV[2] token
-- ARGV[3] max stream len
--
-- Returns 1 (waiting), 0 (no pending children: ready again), or -1 / -2 /
-- -6 like move_to_finished.

local hashKey   = KEYS[2]
local lockKey   = KEYS[3]
local id        = ARGV[1]
local token     = ARGV[2]
local maxStream = tonumber(ARGV[3])

if redis.call('EXISTS', hashKey) == 0 then
  redis.call('LREM', KEYS[1], 0, id)
  redis.call('DEL', lockKey)
  return -1
end
local held = redis.call('GET', lockKey)
if not held then return -2 end
if held ~= token then return -6 end

redis.call('LREM', KEYS[1], 1, id)
redis.call('DEL', lockKey)
redis.call('HDEL', hashKey, 'lockToken', 'processedOn')
if tonumber(redis.call('HGET', hashKey, 'attemptsMade') or '0') > 0 then
  redis.call('HINCRBY', hashKey, 'attemptsMade', -1)
end

local base = oxnSplitKey(hashKey)
local k = oxnQueueKeys(base)
if redis.call('SCARD', hashKey .. ':dependencies') > 0 then
  redis.call('ZADD', KEYS[4], oxnNowMs(), id)
  oxnEmit(k.events, maxStream, 'event', 'waiting-children', 'jobId', id, 'prev', 'active')
  return 1
end
local landed = oxnPushReady(k, id, hashKey, false)
oxnEmit(k.events, maxStream, 'event', 'waiting', 'jobId', id, 'prev', 'active', 'landed', landed)
return 0
