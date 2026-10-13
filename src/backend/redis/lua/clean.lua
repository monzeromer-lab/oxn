-- clean.lua — bulk-remove jobs from a finished/scheduled zset.
--
-- Removes the oldest `limit` entries (at most OXN_BATCH per call) from the
-- target zset along with their job hash, auxiliary keys and dedup pointer.
-- The backend calls it repeatedly for larger or unlimited cleans.
--
-- KEYS[1] target zset (completed | failed | delayed | prioritized |
--                     waiting-children)
-- KEYS[2] events stream (unused: the backend emits one `cleaned` event
--         per clean() call, after all batches)
-- KEYS[3] job-hash prefix (with trailing ':')
--
-- ARGV[1] limit for this call (1..OXN_BATCH)
--
-- Returns: number of jobs removed.

local zsetKey    = KEYS[1]
local eventsKey  = KEYS[2]
local hashPrefix = KEYS[3]

local limit = math.min(tonumber(ARGV[1]), OXN_BATCH)

local ids = redis.call('ZRANGE', zsetKey, 0, limit - 1)
if #ids == 0 then
  return 0
end
redis.call('ZREM', zsetKey, unpack(ids))
for i = 1, #ids do
  oxnRemoveJobKeys(hashPrefix, ids[i])
end
return #ids
