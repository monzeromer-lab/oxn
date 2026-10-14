-- drain.lua — delete up to one batch of pending jobs (not active ones).
--
-- Called repeatedly by the backend until it returns less than a full
-- batch, so draining a huge queue never blocks Redis in one long script.
--
-- KEYS[1] wait
-- KEYS[2] paused
-- KEYS[3] prioritized
-- KEYS[4] delayed
-- KEYS[5] marker
-- KEYS[6] job-hash prefix (trailing ':')
--
-- ARGV[1] include delayed (0|1)
--
-- Returns the number of jobs deleted by this call.

local hashPrefix = KEYS[6]
local includeDelayed = ARGV[1] == '1'
local budget = OXN_BATCH
local removed = 0

local function takeList(key)
  if budget <= 0 then return end
  local ids = redis.call('LRANGE', key, 0, budget - 1)
  if #ids == 0 then return end
  redis.call('LTRIM', key, #ids, -1)
  for i = 1, #ids do oxnRemoveJobKeys(hashPrefix, ids[i]) end
  removed = removed + #ids
  budget = budget - #ids
end

local function takeZset(key)
  if budget <= 0 then return end
  local ids = redis.call('ZRANGE', key, 0, budget - 1)
  if #ids == 0 then return end
  redis.call('ZREM', key, unpack(ids))
  for i = 1, #ids do oxnRemoveJobKeys(hashPrefix, ids[i]) end
  removed = removed + #ids
  budget = budget - #ids
end

takeList(KEYS[1])
takeList(KEYS[2])
takeZset(KEYS[3])
if includeDelayed then takeZset(KEYS[4]) end
if removed < OXN_BATCH then
  redis.call('DEL', KEYS[5]) -- nothing left to wake anyone for
end
return removed
