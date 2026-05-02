-- clean.lua — bulk-remove jobs from a finished/scheduled zset.
--
-- Removes the oldest `limit` entries (or every entry if `limit == 0`) from
-- the target zset along with their job hash + auxiliary keys.
--
-- KEYS[1] target zset (completed | failed | delayed | prioritized |
--                     waiting-children)
-- KEYS[2] events stream
-- KEYS[3] job-hash prefix (with trailing ':')
--
-- ARGV[1] limit (0 = unlimited)
-- ARGV[2] max stream length
--
-- Returns: number of jobs removed.

local zsetKey    = KEYS[1]
local eventsKey  = KEYS[2]
local hashPrefix = KEYS[3]

local limit     = tonumber(ARGV[1])
local maxStream = tonumber(ARGV[2])

local ids
if limit > 0 then
  ids = redis.call('ZRANGE', zsetKey, 0, limit - 1)
else
  ids = redis.call('ZRANGE', zsetKey, 0, -1)
end

if #ids == 0 then
  return 0
end

for i = 1, #ids do
  local id = ids[i]
  local hash = hashPrefix .. id
  redis.call('DEL', hash)
  redis.call('DEL', hash .. ':lock')
  redis.call('DEL', hash .. ':logs')
  redis.call('DEL', hash .. ':dependencies')
  redis.call('DEL', hash .. ':processed')
  redis.call('DEL', hash .. ':unsuccessful')
  redis.call('DEL', hash .. ':failed')
  redis.call('ZREM', zsetKey, id)
end

redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*',
  'event', 'cleaned', 'count', tostring(#ids))

return #ids
