-- add_bulk.lua — add a chunk of jobs in one script call.
--
-- The backend splits larger batches into chunks of at most OXN_BATCH. A
-- chunk runs without other clients interleaving, but like any Redis script
-- it is not rolled back on a runtime error, so inputs are validated before
-- the call.
--
-- KEYS[1]  meta hash
-- KEYS[2]  id counter
-- KEYS[3]  wait list
-- KEYS[4]  paused list
-- KEYS[5]  delayed zset
-- KEYS[6]  prioritized zset
-- KEYS[7]  priority counter
-- KEYS[8]  events stream
-- KEYS[9]  marker key
-- KEYS[10] job-hash prefix (includes trailing ':'; dedup keys hang off it)
--
-- ARGV[1] max stream len
-- ARGV[2] JSON array of jobs: [{ name, data, opts, ts, delay, priority,
--         dedupId, dedupTtl, id, lifo }, …] (data/opts are JSON strings)
--
-- Returns the job ids in input order.

local k = {
  meta = KEYS[1], id = KEYS[2], wait = KEYS[3], paused = KEYS[4],
  delayed = KEYS[5], prio = KEYS[6], pc = KEYS[7], events = KEYS[8],
  marker = KEYS[9], prefix = KEYS[10],
}

local maxStream = tonumber(ARGV[1])
local jobs = cjson.decode(ARGV[2])
local ids = {}
for i = 1, #jobs do
  ids[i] = oxnAddJob(k, jobs[i], maxStream)
end
return ids
