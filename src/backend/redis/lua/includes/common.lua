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
