-- flow.lua — add a whole flow tree in one atomic step.
--
-- Nodes may live in different queues of the same prefix; their keys are
-- derived from each node's queue base (`{prefix}:{queue}`). Every node is
-- written before this script returns, so no worker can see a child
-- without its parent link or a parent without its full dependency set.
--
-- ARGV[1] max stream len
-- ARGV[2] external parent hash key, or '' — the root becomes a child of
--         that (active or waiting-children) job
-- ARGV[3] JSON array of nodes, parents before children:
--   { base, name, data, opts, ts, delay, priority, lifo, id,
--     parent (0-based index of the parent node, -1 for the root),
--     fpof (0|1), idof (0|1), kids (number of children) }
--
-- Returns the ids in node order, or -8 (an explicit id already names a
-- job) / -5 (the external parent does not exist) / -3 (the external
-- parent is neither active nor waiting for children). On an error nothing
-- is written.

local maxStream = tonumber(ARGV[1])
local extParent = ARGV[2]
local nodes = cjson.decode(ARGV[3])

for _, n in ipairs(nodes) do
  if (n.id or '') ~= '' and redis.call('EXISTS', n.base .. ':' .. n.id) == 1 then
    return -8
  end
end
if extParent ~= '' then
  if redis.call('EXISTS', extParent) == 0 then return -5 end
  local pbase, pid = oxnSplitKey(extParent)
  if not redis.call('LPOS', pbase .. ':active', pid)
      and not redis.call('ZSCORE', pbase .. ':waiting-children', pid) then
    return -3
  end
end

local hashKeys, ids = {}, {}
for i, n in ipairs(nodes) do
  local k = oxnQueueKeys(n.base)
  local id = n.id
  if (id or '') == '' then id = tostring(redis.call('INCR', k.id)) end
  local hashKey = k.prefix .. id
  ids[i], hashKeys[i] = id, hashKey
  redis.call('HSET', hashKey,
    'name', n.name, 'data', n.data, 'opts', n.opts,
    'timestamp', n.ts, 'attemptsMade', 0)

  local parentKey
  if n.parent >= 0 then
    parentKey = hashKeys[n.parent + 1]
  elseif extParent ~= '' then
    parentKey = extParent
  end
  if parentKey then
    local pbase, pid = oxnSplitKey(parentKey)
    redis.call('HSET', hashKey, 'parentKey', parentKey,
      'parent', cjson.encode({ id = pid, queue = string.match(pbase, '([^:]+)$') }))
    if tonumber(n.fpof) == 1 then redis.call('HSET', hashKey, 'fpof', '1') end
    if tonumber(n.idof) == 1 then redis.call('HSET', hashKey, 'idof', '1') end
    redis.call('SADD', parentKey .. ':dependencies', hashKey)
  end

  if tonumber(n.kids) > 0 then
    redis.call('ZADD', k.wc, tonumber(n.ts), id)
    oxnEmit(k.events, maxStream, 'event', 'waiting-children', 'jobId', id)
  else
    oxnPlaceNew(k, id, n, maxStream)
  end
  oxnEmit(k.events, maxStream, 'event', 'added', 'jobId', id, 'name', n.name)
end
return ids
