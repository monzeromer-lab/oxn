-- promote_all.lua — move up to one batch of delayed jobs to ready now.
--
-- Each job goes through oxnPushReady (priority and pause respected). The
-- backend calls this repeatedly until it returns less than a full batch,
-- so a huge delayed set never blocks Redis in one script.
--
-- KEYS[1] delayed zset
-- KEYS[2] wait list
-- KEYS[3] events stream
-- KEYS[4] marker zset
-- KEYS[5] meta hash
-- KEYS[6] paused list
-- KEYS[7] prioritized
-- KEYS[8] priority counter
-- KEYS[9] job-hash prefix (trailing ':')
--
-- ARGV[1] max stream length
--
-- Returns the number of jobs promoted by this call.

local delayedKey = KEYS[1]
local eventsKey  = KEYS[3]
local hashPrefix = KEYS[9]
local maxStream  = tonumber(ARGV[1])

local ids = redis.call('ZRANGE', delayedKey, 0, OXN_BATCH - 1)
if #ids == 0 then
  return 0
end
redis.call('ZREM', delayedKey, unpack(ids))

local ready = {
  wait = KEYS[2], paused = KEYS[6], prio = KEYS[7], pc = KEYS[8],
  meta = KEYS[5], marker = KEYS[4],
}
for i = 1, #ids do
  local landed = oxnPushReady(ready, ids[i], hashPrefix .. ids[i], false)
  oxnEmit(eventsKey, maxStream, 'event', 'waiting', 'jobId', ids[i], 'prev', 'delayed', 'landed', landed)
end
return #ids
