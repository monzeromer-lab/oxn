-- pause.lua — pause or resume the queue
--
-- KEYS[1] wait
-- KEYS[2] paused
-- KEYS[3] meta
-- KEYS[4] events
-- ARGV[1] action (pause|resume)
-- ARGV[2] max stream len

local waitKey   = KEYS[1]
local pausedKey = KEYS[2]
local metaKey   = KEYS[3]
local eventsKey = KEYS[4]

local action    = ARGV[1]
local maxStream = tonumber(ARGV[2])

if action == 'pause' then
  if redis.call('EXISTS', waitKey) == 1 then
    -- drain wait into paused (RPOPLPUSH until empty — works even if
    -- paused already has entries)
    while true do
      local id = redis.call('RPOPLPUSH', waitKey, pausedKey)
      if not id then break end
    end
  end
  redis.call('HSET', metaKey, 'paused', '1')
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*', 'event', 'paused')
elseif action == 'resume' then
  if redis.call('EXISTS', pausedKey) == 1 then
    while true do
      local id = redis.call('RPOPLPUSH', pausedKey, waitKey)
      if not id then break end
    end
  end
  redis.call('HSET', metaKey, 'paused', '0')
  redis.call('XADD', eventsKey, 'MAXLEN', '~', maxStream, '*', 'event', 'resumed')
else
  return redis.error_reply('unknown action ' .. tostring(action))
end

return 1
