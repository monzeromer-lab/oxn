-- move_to_active.lua
--
-- Atomically pop the next ready job and acquire a lock.
--
-- Returns { id, hashFields } for a job, { nextDueMs } when nothing is ready
-- but a delayed job will be due in nextDueMs (≥ 1), { 'rl', retryInMs }
-- when the queue's rate limit is exhausted, or {} when there is nothing to
-- wait for (empty or paused). The worker blocks on the marker for at most
-- nextDueMs, so delayed jobs run on time.
--
-- KEYS[1] wait
-- KEYS[2] paused
-- KEYS[3] active
-- KEYS[4] prioritized
-- KEYS[5] delayed
-- KEYS[6] events
-- KEYS[7] meta
-- KEYS[8] marker
-- KEYS[9] job-hash prefix (trailing ':')
-- KEYS[10] priority counter
-- KEYS[11] limiter (fixed-window counter shared by every worker)
--
-- ARGV[1] worker token
-- ARGV[2] lock duration (ms)
-- ARGV[3] now (ms)
-- ARGV[4] max stream len

local waitKey    = KEYS[1]
local pausedKey  = KEYS[2]
local activeKey  = KEYS[3]
local prioKey    = KEYS[4]
local delayedKey = KEYS[5]
local eventsKey  = KEYS[6]
local metaKey    = KEYS[7]
local markerKey  = KEYS[8]
local hashPrefix = KEYS[9]

local token    = ARGV[1]
local lockMs   = tonumber(ARGV[2])
local nowMs    = tonumber(ARGV[3])
local maxStream = tonumber(ARGV[4])
local limitMax  = tonumber(ARGV[5]) or 0   -- 0 = no rate limit
local limitMs   = tonumber(ARGV[6]) or 0

-- short-circuit if paused (resume wakes workers)
if redis.call('HGET', metaKey, 'paused') == '1' then
  return {}
end

-- promote any delayed jobs whose score <= now
-- (through oxnPushReady: a prioritized job stays prioritized)
local due = redis.call('ZRANGEBYSCORE', delayedKey, 0, nowMs, 'LIMIT', 0, 50)
if #due > 0 then
  local ready = {
    wait = waitKey, paused = pausedKey, prio = prioKey, pc = KEYS[10],
    meta = metaKey, marker = markerKey,
  }
  for i = 1, #due do
    redis.call('ZREM', delayedKey, due[i])
    oxnPushReady(ready, due[i], hashPrefix .. due[i], false)
  end
end

-- Rate limit: at most limitMax jobs move to active per limitMs window,
-- counted in one key for every worker on the queue.
local limiterKey = KEYS[11]
if limitMax > 0 then
  local used = tonumber(redis.call('GET', limiterKey) or '0')
  if used >= limitMax then
    local ttl = redis.call('PTTL', limiterKey)
    if ttl < 0 then
      -- counter without expiry (shouldn't happen): start a fresh window
      redis.call('PEXPIRE', limiterKey, limitMs)
      ttl = limitMs
    end
    return { 'rl', ttl }
  end
end

-- Pick a job: prioritized first, then wait (FIFO via RPOPLPUSH).
local id
local popped = redis.call('ZRANGE', prioKey, 0, 0)
local fromPrio = false
if popped and #popped > 0 then
  id = popped[1]
  redis.call('ZREM', prioKey, id)
  fromPrio = true
else
  id = redis.call('RPOPLPUSH', waitKey, activeKey)
  if id == false then id = nil end
end

if id == nil then
  redis.call('DEL', markerKey)
  local nextDue = redis.call('ZRANGE', delayedKey, 0, 0, 'WITHSCORES')
  if #nextDue == 2 then
    local wait = math.floor(tonumber(nextDue[2]) - nowMs)
    if wait < 1 then wait = 1 end
    return { wait }
  end
  return {}
end

-- More ready work left? Wake the next blocked worker (cascade).
oxnRearm(markerKey, waitKey, prioKey)

if limitMax > 0 then
  if redis.call('INCR', limiterKey) == 1 then
    redis.call('PEXPIRE', limiterKey, limitMs)
  end
end

if fromPrio then
  -- prioritized path: we popped from zset, now push to active
  redis.call('LPUSH', activeKey, id)
end

local hashKey = hashPrefix .. id
local lockKey = hashKey .. ':lock'

-- acquire lock
redis.call('SET', lockKey, token, 'PX', lockMs)

-- bookkeeping
redis.call('HSET', hashKey,
  'processedOn', nowMs,
  'lockToken', token)
redis.call('HINCRBY', hashKey, 'attemptsMade', 1)

redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
  'event', 'active', 'jobId', id, 'prev', 'waiting')

local fields = redis.call('HGETALL', hashKey)
return { id, fields }
