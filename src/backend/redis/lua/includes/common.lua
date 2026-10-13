-- common.lua — helpers prepended to every oxn script at compile time
-- (see scripts.rs). Keep it free of side effects: it only defines locals.

-- Wake one worker blocked in BZPOPMIN on the marker.
--
-- The marker is a zset holding a single member, '0'. BZPOPMIN returns as
-- soon as the zset is non-empty, whatever the score, so adding the member
-- wakes exactly one blocked worker. That worker's move_to_active re-arms
-- the marker if work remains (see oxnRearm), waking the next one — a burst
-- therefore cascades through every blocked worker.
local function oxnWake(markerKey)
  redis.call('ZADD', markerKey, 0, '0')
end

-- After a worker took (or failed to take) a job: leave the marker set iff
-- there is still ready work somebody else should wake up for.
local function oxnRearm(markerKey, waitKey, prioKey)
  if redis.call('LLEN', waitKey) > 0 or redis.call('ZCARD', prioKey) > 0 then
    oxnWake(markerKey)
  else
    redis.call('DEL', markerKey)
  end
end

-- Append an event to the queue's stream, trimmed to ~maxStream entries.
local function oxnEmit(eventsKey, maxStream, ...)
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*', ...)
end

-- The job's stored options (ResolvedJobOptions JSON), or {} if absent or
-- unreadable.
local function oxnJobOpts(hashKey)
  local raw = redis.call('HGET', hashKey, 'opts')
  if not raw then return {} end
  local ok, o = pcall(cjson.decode, raw)
  if ok and type(o) == 'table' then return o end
  return {}
end

-- Make an existing job ready again, honoring its priority and the queue's
-- pause state, and wake a worker. `front` puts it at the head of the line
-- (next to be popped) — used for jobs that were already in progress.
--
-- k = { wait=, paused=, prio=, pc=, meta=, marker= }
-- Returns the container it landed in: 'prioritized' | 'wait' | 'paused'.
local function oxnPushReady(k, id, hashKey, front)
  local o = oxnJobOpts(hashKey)
  local prio = tonumber(o['priority']) or 0
  if prio > 0 then
    local counter = redis.call('INCR', k.pc)
    redis.call('ZADD', k.prio, prio * 0x100000000 + counter, id)
    oxnWake(k.marker)
    return 'prioritized'
  end
  local target = k.wait
  if redis.call('HGET', k.meta, 'paused') == '1' then
    target = k.paused
  end
  if front then
    redis.call('RPUSH', target, id)
  else
    redis.call('LPUSH', target, id)
  end
  if target == k.wait then
    oxnWake(k.marker)
    return 'wait'
  end
  return 'paused'
end

-- Most job deletions a single script call performs. Retention trims and
-- bulk removals larger than this are spread over several calls so no one
-- script blocks Redis for long.
local OXN_BATCH = 1000

-- Delete a job's hash and every key hanging off it, plus its dedup pointer
-- if that pointer still names this job. Does not touch containers.
local function oxnRemoveJobKeys(hashPrefix, id)
  local hashKey = hashPrefix .. id
  local d = oxnJobOpts(hashKey)['deduplication']
  if type(d) == 'table' and type(d['id']) == 'string' then
    local dedupKey = hashPrefix .. 'de:' .. d['id']
    if redis.call('GET', dedupKey) == id then
      redis.call('DEL', dedupKey)
    end
  end
  redis.call('DEL', hashKey, hashKey .. ':lock', hashKey .. ':logs',
    hashKey .. ':dependencies', hashKey .. ':processed',
    hashKey .. ':unsuccessful', hashKey .. ':failed')
end

-- Apply a retention policy to a job that just finished into `targetKey`
-- (completed or failed). `policy` is 'keep' | 'remove' | 'last:N' |
-- 'for:MS'. Trims delete the dropped jobs' keys too and are capped at
-- OXN_BATCH per call: a policy change over a huge zset converges over the
-- next few finishes instead of blocking.
local function oxnRetain(targetKey, hashPrefix, id, policy, nowMs)
  if policy == 'remove' then
    oxnRemoveJobKeys(hashPrefix, id)
    return
  end
  redis.call('ZADD', targetKey, nowMs, id)
  local drop
  if string.sub(policy, 1, 5) == 'last:' then
    local n = tonumber(string.sub(policy, 6))
    if n and n >= 0 then
      local excess = redis.call('ZCARD', targetKey) - n
      if excess > 0 then
        drop = redis.call('ZRANGE', targetKey, 0, math.min(excess, OXN_BATCH) - 1)
      end
    end
  elseif string.sub(policy, 1, 4) == 'for:' then
    local ms = tonumber(string.sub(policy, 5))
    if ms and ms > 0 then
      drop = redis.call('ZRANGEBYSCORE', targetKey, 0, nowMs - ms, 'LIMIT', 0, OXN_BATCH)
    end
  end
  if drop and #drop > 0 then
    redis.call('ZREM', targetKey, unpack(drop))
    for i = 1, #drop do oxnRemoveJobKeys(hashPrefix, drop[i]) end
  end
end

-- A `Removal` as serde stores it in the job's opts JSON ("Keep",
-- "Remove", {"KeepLast": n}, {"KeepFor": {"secs": s, "nanos": n}}) →
-- the policy string oxnRetain understands.
local function oxnPolicyFromOpts(r)
  if r == 'Remove' then return 'remove' end
  if type(r) == 'table' then
    if r['KeepLast'] ~= nil then
      return 'last:' .. tostring(math.floor(tonumber(r['KeepLast']) or 0))
    end
    local d = r['KeepFor']
    if type(d) == 'table' then
      local ms = (tonumber(d['secs']) or 0) * 1000 + math.floor((tonumber(d['nanos']) or 0) / 1000000)
      return 'for:' .. tostring(math.floor(ms))
    end
  end
  return 'keep'
end

-- Add one job. Shared by add_job.lua and add_bulk.lua so single and bulk
-- adds behave identically.
--
-- k = { meta=, id=, wait=, paused=, delayed=, prio=, pc=, events=,
--       marker=, prefix= }   (prefix = job-hash prefix with trailing ':')
-- j = { name=, data=, opts=, ts=, delay=, priority=, dedupId=,
--       dedupTtl=, id=, lifo= }   (opts = ResolvedJobOptions JSON)
--
-- Returns the job id: a new one, or the existing job's when the explicit
-- id already exists ('duplicated') or the dedup id is live
-- ('deduplicated').
local function oxnAddJob(k, j, maxStream)
  local dedupId = j.dedupId or ''
  local dedupKey = k.prefix .. 'de:' .. dedupId
  if dedupId ~= '' then
    local existing = redis.call('GET', dedupKey)
    if existing then
      oxnEmit(k.events, maxStream, 'event', 'deduplicated', 'jobId', existing, 'dedupId', dedupId)
      return existing
    end
  end

  local id
  if (j.id or '') ~= '' then
    id = j.id
    -- An explicit id that already names a job returns that job untouched:
    -- re-submitting is idempotent, never an overwrite or a second enqueue.
    if redis.call('EXISTS', k.prefix .. id) == 1 then
      oxnEmit(k.events, maxStream, 'event', 'duplicated', 'jobId', id)
      return id
    end
  else
    id = tostring(redis.call('INCR', k.id))
  end

  local ts = tonumber(j.ts)
  redis.call('HSET', k.prefix .. id,
    'name', j.name,
    'data', j.data,
    'opts', j.opts,
    'timestamp', ts,
    'attemptsMade', 0)

  local ttl = tonumber(j.dedupTtl) or 0
  if dedupId ~= '' and ttl > 0 then
    redis.call('SET', dedupKey, id, 'PX', ttl)
  end

  local delay = tonumber(j.delay) or 0
  local priority = tonumber(j.priority) or 0
  if delay > 0 then
    local due = ts + delay
    redis.call('ZADD', k.delayed, due, id)
    -- Wake a blocked worker so it re-computes how long to block: it may be
    -- sleeping for a full drain_delay that ends after this job is due.
    oxnWake(k.marker)
    oxnEmit(k.events, maxStream, 'event', 'delayed', 'jobId', id, 'delay', due)
  elseif priority > 0 then
    local counter = redis.call('INCR', k.pc)
    -- priority in the high bits, a counter below for FIFO within a tier
    redis.call('ZADD', k.prio, priority * 0x100000000 + counter, id)
    oxnWake(k.marker)
  else
    local target = k.wait
    if redis.call('HGET', k.meta, 'paused') == '1' then
      target = k.paused
    end
    if tonumber(j.lifo) == 1 then
      redis.call('RPUSH', target, id)
    else
      redis.call('LPUSH', target, id)
    end
    if target == k.wait then
      oxnWake(k.marker)
    end
    oxnEmit(k.events, maxStream, 'event', 'waiting', 'jobId', id)
  end

  oxnEmit(k.events, maxStream, 'event', 'added', 'jobId', id, 'name', j.name)
  return id
end
