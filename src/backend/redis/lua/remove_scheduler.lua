-- remove_scheduler.lua — remove a scheduler and every pending tick.
--
-- The record becomes a tombstone (keeping its high-water mark), so every
-- running instance of the scheduler stops at its next tick instead of
-- re-creating it; a scheduler started afterwards registers again. Every
-- tick still waiting to run is removed (there can be several: instances
-- starting around a tick boundary, or ticks piling up while no worker
-- runs); a tick a worker is already running is left to finish.
--
-- KEYS[1] schedulers hash
-- KEYS[2] wait   KEYS[3] paused   KEYS[4] delayed   KEYS[5] prioritized
-- KEYS[6] events KEYS[7] job-hash prefix (trailing ':')
-- KEYS[8] this scheduler's ticks zset   KEYS[9] active
-- ARGV[1] scheduler id   ARGV[2] now (ms)   ARGV[3] max stream len
--
-- Returns the number of pending ticks removed + 1, or 0 if there was no
-- such (live) scheduler.

local sid = ARGV[1]
local maxStream = tonumber(ARGV[3])
local raw = redis.call('HGET', KEYS[1], sid)
if not raw then return 0 end
local cur = cjson.decode(raw)
if cur['removed_ms'] then return 0 end

local removed = 0
for _, t in ipairs(redis.call('ZRANGE', KEYS[8], 0, -1)) do
  local id = 'sched-' .. sid .. '-' .. string.format('%.0f', tonumber(t))
  local h = KEYS[7] .. id
  if redis.call('EXISTS', h) == 1 and not redis.call('HGET', h, 'finishedOn')
      and not redis.call('LPOS', KEYS[9], id) then
    redis.call('LREM', KEYS[2], 0, id)
    redis.call('LREM', KEYS[3], 0, id)
    redis.call('ZREM', KEYS[4], id)
    redis.call('ZREM', KEYS[5], id)
    oxnRemoveJobKeys(KEYS[7], id, maxStream)
    oxnEmit(KEYS[6], maxStream, 'event', 'removed', 'jobId', id)
    removed = removed + 1
  end
end
redis.call('DEL', KEYS[8])
redis.call('HSET', KEYS[1], sid, cjson.encode({
  id = sid, next_ms = cur['next_ms'], removed_ms = tonumber(ARGV[2]),
}))
return removed + 1
