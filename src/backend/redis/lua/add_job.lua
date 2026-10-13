-- add_job.lua — add one job (logic shared with add_bulk: oxnAddJob).
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
-- ARGV[1]  name
-- ARGV[2]  data (JSON)
-- ARGV[3]  opts (JSON)
-- ARGV[4]  timestamp (ms)
-- ARGV[5]  delay (ms)
-- ARGV[6]  priority (int)
-- ARGV[7]  dedup id (may be empty string)
-- ARGV[8]  dedup TTL (ms)
-- ARGV[9]  explicit id (may be empty)
-- ARGV[10] lifo (0|1)
-- ARGV[11] max stream len

local k = {
  meta = KEYS[1], id = KEYS[2], wait = KEYS[3], paused = KEYS[4],
  delayed = KEYS[5], prio = KEYS[6], pc = KEYS[7], events = KEYS[8],
  marker = KEYS[9], prefix = KEYS[10],
}

return oxnAddJob(k, {
  name = ARGV[1], data = ARGV[2], opts = ARGV[3], ts = ARGV[4],
  delay = ARGV[5], priority = ARGV[6], dedupId = ARGV[7],
  dedupTtl = ARGV[8], id = ARGV[9], lifo = ARGV[10],
}, tonumber(ARGV[11]))
