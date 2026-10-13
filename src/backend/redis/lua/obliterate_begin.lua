-- obliterate_begin.lua — first, atomic step of obliterate().
--
-- Checks for active jobs and, if the obliterate may proceed, pauses the
-- queue in the same step: from here on no worker can move a job to
-- active, so none can be deleted out from under one while the backend
-- deletes the queue's keys in batches. The backend deletes `meta` (and
-- with it the pause flag) last.
--
-- KEYS[1] active
-- KEYS[2] meta
--
-- ARGV[1] force (1 = proceed even with active jobs)
--
-- Returns 0 when the obliterate may proceed, else the number of active
-- jobs that blocked it (nothing was changed).

local active = redis.call('LLEN', KEYS[1])
if ARGV[1] ~= '1' and active > 0 then
  return active
end
redis.call('HSET', KEYS[2], 'paused', '1')
return 0
