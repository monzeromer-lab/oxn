-- scan_stalled.lua
--
-- Detect jobs in `active` whose lock has expired (their worker died, hung
-- past lock_duration, or lost its connection).
--
-- * Up to max_stalled stalls: the job goes back to the head of its ready
--   container (priority and pause respected) and the interrupted attempt
--   is refunded — stalls have their own budget (max_stalled) and never
--   eat into `attempts`.
-- * Beyond max_stalled: the job fails for good into `failed`, with its own
--   remove_on_fail retention applied, exactly like a handler failure.
--
-- KEYS[1]  active
-- KEYS[2]  wait
-- KEYS[3]  stalled (legacy set, deleted)
-- KEYS[4]  stalled_check (rate-limit flag)
-- KEYS[5]  events
-- KEYS[6]  job-hash prefix
-- KEYS[7]  marker
-- KEYS[8]  paused
-- KEYS[9]  prioritized
-- KEYS[10] priority counter
-- KEYS[11] meta
-- KEYS[12] failed
--
-- ARGV[1] now (ms)
-- ARGV[2] max stalled
-- ARGV[3] max stream len
--
-- Returns {requeued ids, failed} where failed is a flat list of
-- (id, name, attemptsMade, attempts) for each job failed for good, read
-- before its retention policy may delete it.

local activeKey  = KEYS[1]
local eventsKey  = KEYS[5]
local hashPrefix = KEYS[6]
local failedKey  = KEYS[12]

local nowMs      = tonumber(ARGV[1])
local maxStalled = tonumber(ARGV[2])
local maxStream  = tonumber(ARGV[3])

-- run at most once per second per queue; exit early otherwise
if not redis.call('SET', KEYS[4], nowMs, 'NX', 'PX', 1000) then
  return {{}, {}}
end

-- The legacy `stalled` set was write-only; nothing ever read it.
redis.call('DEL', KEYS[3])

local ready = {
  wait = KEYS[2], paused = KEYS[8], prio = KEYS[9], pc = KEYS[10],
  meta = KEYS[11], marker = KEYS[7],
}
local reason = 'job stalled more than allowable limit'

local moved = {}
local failed = {}
local active = redis.call('LRANGE', activeKey, 0, -1)
for i = 1, #active do
  local id = active[i]
  local hashKey = hashPrefix .. id
  if redis.call('EXISTS', hashKey .. ':lock') == 0 then
    redis.call('LREM', activeKey, 1, id)
    redis.call('HDEL', hashKey, 'lockToken', 'processedOn')
    local stalled = tonumber(redis.call('HINCRBY', hashKey, 'stalledCounter', 1))
    if stalled > maxStalled then
      redis.call('HSET', hashKey, 'failedReason', reason, 'finishedOn', nowMs)
      local attemptsMade = redis.call('HGET', hashKey, 'attemptsMade') or '0'
      oxnPushStack(hashKey, 'attempt ' .. attemptsMade .. ': ' .. reason
        .. ' (stalled ' .. stalled .. ' times)')
      local opts = oxnJobOpts(hashKey)
      local policy = oxnPolicyFromOpts(opts['remove_on_fail'])
      table.insert(failed, id)
      table.insert(failed, redis.call('HGET', hashKey, 'name') or '')
      table.insert(failed, attemptsMade)
      table.insert(failed, tostring(tonumber(opts['attempts']) or 1))
      oxnChildFailed(hashKey, reason, nowMs, maxStream)
      oxnRetain(failedKey, hashPrefix, id, policy, nowMs)
      oxnEmit(eventsKey, maxStream, 'event', 'failed', 'jobId', id,
        'failedReason', reason, 'attemptsMade', attemptsMade, 'stalled', tostring(stalled))
    else
      if tonumber(redis.call('HGET', hashKey, 'attemptsMade') or '0') > 0 then
        redis.call('HINCRBY', hashKey, 'attemptsMade', -1)
      end
      local landed = oxnPushReady(ready, id, hashKey, true)
      oxnEmit(eventsKey, maxStream, 'event', 'stalled', 'jobId', id,
        'stalledCounter', tostring(stalled), 'requeued', landed)
      table.insert(moved, id)
    end
  end
end
return {moved, failed}
