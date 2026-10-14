-- remove.lua — delete a job, and the flow tree below it, from every
-- container and drop their keys.
--
-- Returns the number of jobs removed, or -1 (no such job) / -7 (the job or
-- one of its descendants is active and locked; nothing is removed).
--
-- A parent's children (pending, processed or failed — they may live in
-- other queues of the same prefix) are removed with it, recursively. A
-- removed child that its own parent still waits on resolves that
-- dependency (see oxnRemoveJobKeys).
--
-- KEYS[1] job hash
-- ARGV[1] id
-- ARGV[2] max stream len

local rootKey   = KEYS[1]
local maxStream = tonumber(ARGV[2])

if redis.call('EXISTS', rootKey) == 0 then return -1 end

-- The whole tree, parents before children.
local tree = { rootKey }
local seen = { [rootKey] = true }
local i = 1
while i <= #tree do
  local key = tree[i]
  local kids = redis.call('SMEMBERS', key .. ':dependencies')
  for _, c in ipairs(redis.call('HKEYS', key .. ':processed')) do table.insert(kids, c) end
  for _, c in ipairs(redis.call('HKEYS', key .. ':failed')) do table.insert(kids, c) end
  for _, c in ipairs(kids) do
    if not seen[c] and redis.call('EXISTS', c) == 1 then
      seen[c] = true
      table.insert(tree, c)
    end
  end
  i = i + 1
end

-- -7: a job in the tree is active and a worker holds its lock — deleting
-- it would pull the job out from under that worker.
for _, key in ipairs(tree) do
  local base, id = oxnSplitKey(key)
  if redis.call('EXISTS', key .. ':lock') == 1
      and redis.call('LPOS', base .. ':active', id) then
    return -7
  end
end

for _, key in ipairs(tree) do
  local base, id = oxnSplitKey(key)
  local k = oxnQueueKeys(base)
  redis.call('LREM', k.wait, 0, id)
  redis.call('LREM', k.paused, 0, id)
  redis.call('LREM', k.active, 0, id)
  redis.call('ZREM', k.delayed, id)
  redis.call('ZREM', k.prio, id)
  redis.call('ZREM', k.completed, id)
  redis.call('ZREM', k.failed, id)
  redis.call('ZREM', k.wc, id)
  oxnRemoveJobKeys(k.prefix, id, maxStream)
  oxnEmit(k.events, maxStream, 'event', 'removed', 'jobId', id)
end
return #tree
