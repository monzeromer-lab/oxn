-- schedule.lua — enqueue one scheduler tick, exactly once.
--
-- Every instance of a scheduler computes the same fire times, and tick T
-- is the job `sched-{id}-{T}`. The scheduler's record in the `schedulers`
-- hash keeps a high-water mark (`next_ms`, the latest tick enqueued); a
-- tick at or below it is never added again, so N instances produce one
-- job per tick — even after a fired tick's job was deleted by retention.
--
-- KEYS[1]  schedulers hash
-- KEYS[2..11] as add_job.lua KEYS[1..10]
-- KEYS[12] this scheduler's ticks zset (`schedulers:{id}`): fire times of
--          ticks that may still be pending, pruned a few at a time here,
--          so remove_scheduler can find every pending tick
--
-- ARGV[1]  scheduler id
-- ARGV[2]  fire time T (ms)
-- ARGV[3]  record JSON ({ id, repeat, name })
-- ARGV[4]  register: 1 when a scheduler (re)starts — revives a removed
--          one; 0 for a running loop — a removed scheduler stays removed
-- ARGV[5]  job id (`sched-{id}-{T}`)
-- ARGV[6]  name     ARGV[7]  data     ARGV[8]  opts (JSON)
-- ARGV[9]  timestamp (ms)    ARGV[10] delay (ms)
-- ARGV[11] priority  ARGV[12] lifo   ARGV[13] max stream len
--
-- Returns { status, next_ms }: status 'added' (this call enqueued tick T),
-- 'exists' (T was already enqueued — by another instance or earlier; the
-- latest tick is next_ms), or 'removed' (the scheduler was removed: stop).

local schedKey = KEYS[1]
local sid      = ARGV[1]
local fire     = tonumber(ARGV[2])

local raw = redis.call('HGET', schedKey, sid)
local cur = raw and cjson.decode(raw) or {}
if cur['removed_ms'] and ARGV[4] ~= '1' then
  return { 'removed', tonumber(cur['next_ms']) or 0 }
end
local mark = tonumber(cur['next_ms']) or -1
if fire <= mark then
  return { 'exists', mark }
end

local rec = cjson.decode(ARGV[3])
rec['next_ms'] = fire
rec['updated_ms'] = tonumber(ARGV[9])
redis.call('HSET', schedKey, sid, cjson.encode(rec))

local k = {
  meta = KEYS[2], id = KEYS[3], wait = KEYS[4], paused = KEYS[5],
  delayed = KEYS[6], prio = KEYS[7], pc = KEYS[8], events = KEYS[9],
  marker = KEYS[10], prefix = KEYS[11],
}
oxnAddJob(k, {
  name = ARGV[6], data = ARGV[7], opts = ARGV[8], ts = ARGV[9],
  delay = ARGV[10], priority = ARGV[11], dedupId = '', dedupTtl = 0,
  id = ARGV[5], lifo = ARGV[12],
}, tonumber(ARGV[13]))

-- Remember the tick; forget up to 10 of the oldest that are no longer
-- pending (started, finished or deleted).
local ticksKey = KEYS[12]
redis.call('ZADD', ticksKey, fire, fire)
for _, t in ipairs(redis.call('ZRANGE', ticksKey, 0, 9)) do
  local h = KEYS[11] .. 'sched-' .. sid .. '-' .. string.format('%.0f', tonumber(t))
  local f = redis.call('HMGET', h, 'processedOn', 'finishedOn')
  if redis.call('EXISTS', h) == 0 or f[1] or f[2] then
    redis.call('ZREM', ticksKey, t)
  end
end
return { 'added', fire }
