-- job_update.lua — record progress or append a log line for a job.
--
-- Refuses (-1) when the job no longer exists: a plain HSET / RPUSH would
-- silently re-create an orphan hash or `:logs` key for a removed job.
--
-- KEYS[1] job hash
-- KEYS[2] job logs
-- KEYS[3] events
--
-- ARGV[1] kind ('progress' | 'log')
-- ARGV[2] value (progress JSON, or the log line)
-- ARGV[3] job id (for the event)
-- ARGV[4] max stream len

if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if ARGV[1] == 'progress' then
  redis.call('HSET', KEYS[1], 'progress', ARGV[2])
  oxnEmit(KEYS[3], tonumber(ARGV[4]), 'event', 'progress', 'jobId', ARGV[3], 'data', ARGV[2])
else
  redis.call('RPUSH', KEYS[2], ARGV[2])
end
return 1
