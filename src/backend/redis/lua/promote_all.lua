-- promote_all.lua — move every entry in `delayed` to `wait` immediately.
--
-- KEYS[1] delayed zset
-- KEYS[2] wait list
-- KEYS[3] events stream
-- KEYS[4] marker zset (wakes blocked workers)
-- KEYS[5] meta hash
-- KEYS[6] paused list (target if the queue is currently paused)
--
-- ARGV[1] max stream length
--
-- Returns: number of jobs promoted.

local delayedKey = KEYS[1]
local waitKey    = KEYS[2]
local eventsKey  = KEYS[3]
local markerKey  = KEYS[4]
local metaKey    = KEYS[5]
local pausedKey  = KEYS[6]

local maxStream = tonumber(ARGV[1])

local ids = redis.call('ZRANGE', delayedKey, 0, -1)
if #ids == 0 then
  return 0
end

-- Honour pause: if the queue is paused, the promoted jobs go straight to
-- the paused list so they don't run until the queue resumes.
local target = waitKey
if redis.call('HGET', metaKey, 'paused') == '1' then
  target = pausedKey
end

for i = 1, #ids do
  local id = ids[i]
  redis.call('LPUSH', target, id)
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
    'event', 'waiting', 'jobId', id)
end

-- Wipe the delayed zset (we just emptied it) and bump the marker so any
-- worker blocking on BZPOPMIN wakes up and tries to fetch.
redis.call('DEL', delayedKey)
redis.call('ZADD', markerKey, 0, '0')

return #ids
