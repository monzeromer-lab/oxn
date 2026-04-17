-- extend_lock.lua
--
-- KEYS[1] job lock
-- ARGV[1] token
-- ARGV[2] duration (ms)

local lockKey = KEYS[1]
local token   = ARGV[1]
local ms      = tonumber(ARGV[2])

local held = redis.call('GET', lockKey)
if held == false or held == nil then return -2 end
if held ~= token then return -6 end
redis.call('PEXPIRE', lockKey, ms)
return 1
