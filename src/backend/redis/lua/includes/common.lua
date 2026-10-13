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
