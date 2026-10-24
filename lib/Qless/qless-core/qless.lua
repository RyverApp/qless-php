-- Current SHA: c877b3c6a1be14244a6a9ec3219ce7f8911e84a9
-- This is a generated file
local Qless = {
  ns = 'ql:',
}

local QlessQueue = {
  ns = Qless.ns .. 'q:',
}
QlessQueue.__index = QlessQueue

local QlessWorker = {
  ns = Qless.ns .. 'w:',
}
QlessWorker.__index = QlessWorker

local QlessJob = {
  ns = Qless.ns .. 'j:',
}
QlessJob.__index = QlessJob

local QlessRecurringJob = {}
QlessRecurringJob.__index = QlessRecurringJob

local QlessResource = {
  ns = Qless.ns .. 'rs:',
}
QlessResource.__index = QlessResource

Qless.config = {}

local function extend_table(target, other)
  for _, v in ipairs(other) do
    table.insert(target, v)
  end
end

local function hmset(key, fields)
  local args = { key }
  for field, value in pairs(fields) do
    table.insert(args, field)
    table.insert(args, value)
  end
  return redis.call('hmset', unpack(args))
end

function Qless.publish(channel, message)
  redis.call('publish', Qless.ns .. channel, message)
end

function Qless.job(jid)
  assert(jid, 'Job(): no jid provided')
  local job = {}
  setmetatable(job, QlessJob)
  job.jid = jid
  return job
end

function Qless.recurring(jid)
  assert(jid, 'Recurring(): no jid provided')
  local job = {}
  setmetatable(job, QlessRecurringJob)
  job.jid = jid
  return job
end

function Qless.resource(rid)
  assert(rid, 'Resource(): no rid provided')
  local res = {}
  setmetatable(res, QlessResource)
  res.rid = rid
  return res
end

function Qless.failed(group, start, limit)
  start = assert(tonumber(start or 0), 'Failed(): Arg "start" is not a number: ' .. (start or 'nil'))
  limit = assert(tonumber(limit or 25), 'Failed(): Arg "limit" is not a number: ' .. (limit or 'nil'))

  if group then
    return {
      total = redis.call('llen', 'ql:f:' .. group),
      jobs = redis.call('lrange', 'ql:f:' .. group, start, start + limit - 1),
    }
  else
    local response = {}
    local groups = redis.call('smembers', 'ql:failures')
    for _, failure_group in ipairs(groups) do
      response[failure_group] = redis.call('llen', 'ql:f:' .. failure_group)
    end
    return response
  end
end

function Qless.jobs(now, state, ...)
  assert(state, 'Jobs(): Arg "state" missing')
  if state == 'complete' then
    local offset, count = ...
    offset = assert(tonumber(offset or 0), 'Jobs(): Arg "offset" not a number: ' .. tostring(offset))
    count = assert(tonumber(count or 25), 'Jobs(): Arg "count" not a number: ' .. tostring(count))
    return redis.call('zrevrange', 'ql:completed', offset, offset + count - 1)
  else
    local name, offset, count = ...
    name = assert(name, 'Jobs(): Arg "queue" missing')
    offset = assert(tonumber(offset or 0), 'Jobs(): Arg "offset" not a number: ' .. tostring(offset))
    count = assert(tonumber(count or 25), 'Jobs(): Arg "count" not a number: ' .. tostring(count))

    local queue = Qless.queue(name)
    if state == 'running' then
      return queue.locks.peek(now, offset, count)
    elseif state == 'stalled' then
      return queue.locks.expired(now, offset, count)
    elseif state == 'waiting' then
      return queue.work.peek(now, offset, count)
    elseif state == 'scheduled' then
      queue:check_scheduled(now, queue.scheduled.length())
      return queue.scheduled.peek(now, offset, count)
    elseif state == 'depends' then
      return queue.depends.peek(now, offset, count)
    elseif state == 'recurring' then
      return queue.recurring.peek('+inf', offset, count)
    else
      error('Jobs(): Unknown type "' .. state .. '"')
    end
  end
end

function Qless.track(now, command, jid)
  if command ~= nil then
    assert(jid, 'Track(): Arg "jid" missing')
    assert(Qless.job(jid):exists(), 'Track(): Job does not exist')
    if string.lower(command) == 'track' then
      Qless.publish('track', jid)
      return redis.call('zadd', 'ql:tracked', now, jid)
    elseif string.lower(command) == 'untrack' then
      Qless.publish('untrack', jid)
      return redis.call('zrem', 'ql:tracked', jid)
    else
      error('Track(): Unknown action "' .. command .. '"')
    end
  else
    local response = {
      jobs = {},
      expired = {},
    }
    local jids = redis.call('zrange', 'ql:tracked', 0, -1)
    for _, tracked_jid in ipairs(jids) do
      local data = Qless.job(tracked_jid):data()
      if data then
        table.insert(response.jobs, data)
      else
        table.insert(response.expired, tracked_jid)
      end
    end
    return response
  end
end

function Qless.tag(now, command, ...)
  assert(command, 'Tag(): Arg "command" must be "add", "remove", "get" or "top"')

  if command == 'add' then
    local jid = assert((...), 'Tag(): Arg "jid" missing')
    local tags = redis.call('hget', QlessJob.ns .. jid, 'tags')
    if tags then
      tags = cjson.decode(tags)
      local _tags = {}
      for _, v in ipairs(tags) do
        _tags[v] = true
      end

      for i = 2, select('#', ...) do
        local tag = select(i, ...)
        if not _tags[tag] then
          _tags[tag] = true
          table.insert(tags, tag)
        end
        redis.call('zadd', 'ql:t:' .. tag, now, jid)
        redis.call('zincrby', 'ql:tags', 1, tag)
      end

      redis.call('hset', QlessJob.ns .. jid, 'tags', cjson.encode(tags))
      return tags
    else
      error('Tag(): Job ' .. jid .. ' does not exist')
    end
  elseif command == 'remove' then
    local jid = assert((...), 'Tag(): Arg "jid" missing')
    local tags = redis.call('hget', QlessJob.ns .. jid, 'tags')
    if tags then
      tags = cjson.decode(tags)
      local _tags = {}
      for _, v in ipairs(tags) do
        _tags[v] = true
      end

      for i = 2, select('#', ...) do
        local tag = select(i, ...)
        _tags[tag] = nil
        redis.call('zrem', 'ql:t:' .. tag, jid)
        redis.call('zincrby', 'ql:tags', -1, tag)
      end

      local results = {}
      for _, tag in ipairs(tags) do
        if _tags[tag] then
          table.insert(results, tag)
        end
      end

      redis.call('hset', QlessJob.ns .. jid, 'tags', cjson.encode(results))
      return results
    else
      error('Tag(): Job ' .. jid .. ' does not exist')
    end
  elseif command == 'get' then
    local tag, offset, count = ...
    assert(tag, 'Tag(): Arg "tag" missing')
    offset = assert(tonumber(offset or 0), 'Tag(): Arg "offset" not a number: ' .. tostring(offset))
    count = assert(tonumber(count or 25), 'Tag(): Arg "count" not a number: ' .. tostring(count))
    return {
      total = redis.call('zcard', 'ql:t:' .. tag),
      jobs = redis.call('zrange', 'ql:t:' .. tag, offset, offset + count - 1),
    }
  elseif command == 'top' then
    local offset, count = ...
    offset = assert(tonumber(offset or 0), 'Tag(): Arg "offset" not a number: ' .. tostring(offset))
    count = assert(tonumber(count or 25), 'Tag(): Arg "count" not a number: ' .. tostring(count))
    return redis.call('zrevrangebyscore', 'ql:tags', '+inf', 2, 'limit', offset, count)
  else
    error('Tag(): First argument must be "add", "remove" or "get"')
  end
end

function Qless.cancel(now, ...)
  local dependents = {}
  for i = 1, select('#', ...) do
    local jid = select(i, ...)
    dependents[jid] = redis.call('smembers', QlessJob.ns .. jid .. '-dependents') or {}
  end

  for i = 1, select('#', ...) do
    local jid = select(i, ...)
    for _, dep in ipairs(dependents[jid]) do
      if not dependents[dep] then
        error('Cancel(): ' .. jid .. ' is a dependency of ' .. dep .. ' but is not mentioned to be canceled')
      end
    end
  end

  local canceled_jids = {}

  for i = 1, select('#', ...) do
    local jid = select(i, ...)
    local state, queue, failure, worker =
      unpack(redis.call('hmget', QlessJob.ns .. jid, 'state', 'queue', 'failure', 'worker'))

    if state ~= false and state ~= 'complete' then
      table.insert(canceled_jids, jid)

      local encoded = cjson.encode({
        jid = jid,
        worker = worker,
        event = 'canceled',
        queue = queue,
      })
      Qless.publish('log', encoded)

      if worker and (worker ~= '') then
        redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)
        Qless.publish('w:' .. worker, encoded)
      end

      if queue then
        local queue_obj = Qless.queue(queue)
        queue_obj.work.remove(jid)
        queue_obj.locks.remove(jid)
        queue_obj.scheduled.remove(jid)
        queue_obj.depends.remove(jid)
      end

      Qless.job(jid):release_resources(now)

      for _, j in ipairs(redis.call('smembers', QlessJob.ns .. jid .. '-dependencies')) do
        redis.call('srem', QlessJob.ns .. j .. '-dependents', jid)
      end

      redis.call('del', QlessJob.ns .. jid .. '-dependencies')

      if state == 'failed' then
        failure = cjson.decode(failure)
        redis.call('lrem', 'ql:f:' .. failure.group, 0, jid)
        if redis.call('llen', 'ql:f:' .. failure.group) == 0 then
          redis.call('srem', 'ql:failures', failure.group)
        end
        local bin = failure.when - (failure.when % 86400)
        local failed = redis.call('hget', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed')
        redis.call('hset', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed', failed - 1)
      end

      local tags = cjson.decode(redis.call('hget', QlessJob.ns .. jid, 'tags') or '{}')
      for _, tag in ipairs(tags) do
        redis.call('zrem', 'ql:t:' .. tag, jid)
        redis.call('zincrby', 'ql:tags', -1, tag)
      end

      if redis.call('zscore', 'ql:tracked', jid) ~= false then
        Qless.publish('canceled', jid)
      end

      redis.call('del', QlessJob.ns .. jid)
      redis.call('del', QlessJob.ns .. jid .. '-history')
    end
  end

  return canceled_jids
end
local Set = {}

function Set.new(t)
  local set = {}
  for _, l in ipairs(t) do
    set[l] = true
  end
  return set
end

function Set.union(a, b)
  local res = Set.new({})
  for k in pairs(a) do
    res[k] = true
  end
  for k in pairs(b) do
    res[k] = true
  end
  return res
end

function Set.intersection(a, b)
  local res = Set.new({})
  for k in pairs(a) do
    res[k] = b[k]
  end
  return res
end

function Set.diff(a, b)
  local res = Set.new({})
  for k in pairs(a) do
    if not b[k] then
      res[k] = true
    end
  end

  return res
end

Qless.config.defaults = {
  ['application'] = 'qless',
  ['heartbeat'] = 60,
  ['grace-period'] = 10,
  ['stats-history'] = 30,
  ['histogram-history'] = 7,
  ['jobs-history-count'] = 50000,
  ['jobs-history'] = 604800,
}

Qless.config.get = function(key, default)
  if key then
    return redis.call('hget', 'ql:config', key) or Qless.config.defaults[key] or default
  else
    local reply = redis.call('hgetall', 'ql:config')
    for i = 1, #reply, 2 do
      Qless.config.defaults[reply[i]] = reply[i + 1]
    end
    return Qless.config.defaults
  end
end

Qless.config.set = function(option, value)
  assert(option, 'config.set(): Arg "option" missing')
  assert(value, 'config.set(): Arg "value" missing')
  Qless.publish(
    'log',
    cjson.encode({
      event = 'config_set',
      option = option,
      value = value,
    })
  )

  redis.call('hset', 'ql:config', option, value)
end

Qless.config.unset = function(option)
  assert(option, 'config.unset(): Arg "option" missing')
  Qless.publish(
    'log',
    cjson.encode({
      event = 'config_unset',
      option = option,
    })
  )

  redis.call('hdel', 'ql:config', option)
end

function QlessJob:data()
  local job = redis.call(
    'hmget',
    QlessJob.ns .. self.jid,
    'jid',
    'klass',
    'state',
    'queue',
    'worker',
    'priority',
    'expires',
    'retries',
    'remaining',
    'data',
    'tags',
    'failure',
    'spawned_from_jid',
    'resources',
    'result_data',
    'throttle_interval'
  )

  if not job[1] then
    return nil
  end

  local dependents = redis.call('smembers', QlessJob.ns .. self.jid .. '-dependents')
  local dependencies = redis.call('smembers', QlessJob.ns .. self.jid .. '-dependencies')
  table.sort(dependents)
  table.sort(dependencies)

  return {
    jid = job[1],
    klass = job[2],
    state = job[3],
    queue = job[4],
    worker = job[5] or '',
    tracked = redis.call('zscore', 'ql:tracked', self.jid) ~= false,
    priority = tonumber(job[6]),
    expires = tonumber(job[7]) or 0,
    retries = tonumber(job[8]),
    remaining = math.floor(tonumber(job[9]) or 0),
    data = job[10],
    tags = cjson.decode(job[11]),
    history = self:history(),
    failure = cjson.decode(job[12] or '{}'),
    resources = cjson.decode(job[14] or '[]'),
    result_data = cjson.decode(job[15] or '{}'),
    interval = tonumber(job[16]) or 0,
    dependents = dependents,
    dependencies = dependencies,
    spawned_from_jid = job[13],
  }
end

function QlessJob:complete(now, worker, queue, data, ...)
  assert(worker, 'Complete(): Arg "worker" missing')
  assert(queue, 'Complete(): Arg "queue" missing')
  if data then
    assert(cjson.decode(data), 'Complete(): Arg "data" not JSON: ' .. tostring(data))
  end

  local arg_count = select('#', ...)
  if arg_count % 2 == 1 then
    error('Odd number of additional args')
  end
  local options = {}
  for i = 1, arg_count, 2 do
    local key, val = select(i, ...)
    options[key] = val
  end

  local nextq = options['next']
  local delay = assert(tonumber(options['delay'] or 0))
  local depends = assert(
    cjson.decode(options['depends'] or '[]'),
    'Complete(): Arg "depends" not JSON: ' .. tostring(options['depends'])
  )

  local result_data = options['result_data']

  if options['delay'] and nextq == nil then
    error('Complete(): "delay" cannot be used without a "next".')
  end

  if options['depends'] and nextq == nil then
    error('Complete(): "depends" cannot be used without a "next".')
  end

  local lastworker, state, priority, retries, current_queue, interval = unpack(
    redis.call('hmget', QlessJob.ns .. self.jid, 'worker', 'state', 'priority', 'retries', 'queue', 'throttle_interval')
  )

  if lastworker == false then
    error('Complete(): Job ' .. self.jid .. ' does not exist')
  elseif state ~= 'running' then
    error('Complete(): Job ' .. self.jid .. ' is not currently running: ' .. state)
  elseif lastworker ~= worker then
    error('Complete(): Job ' .. self.jid .. ' has been handed out to another worker: ' .. tostring(lastworker))
  elseif queue ~= current_queue then
    error('Complete(): Job ' .. self.jid .. ' running in another queue: ' .. tostring(current_queue))
  end

  local next_run = 0
  if interval then
    interval = tonumber(interval)
    if interval > 0 then
      next_run = now + interval
    else
      next_run = -1
    end
  end

  self:history(now, 'done')

  if data then
    redis.call('hset', QlessJob.ns .. self.jid, 'data', data)
  end

  if result_data then
    redis.call('hset', QlessJob.ns .. self.jid, 'result_data', result_data)
  end

  local queue_obj = Qless.queue(queue)
  queue_obj.work.remove(self.jid)
  queue_obj.locks.remove(self.jid)
  queue_obj.scheduled.remove(self.jid)

  self:release_resources(now)

  local time = tonumber(redis.call('hget', QlessJob.ns .. self.jid, 'time') or now)
  local waiting = now - time
  Qless.queue(queue):stat(now, 'run', waiting)
  redis.call('hset', QlessJob.ns .. self.jid, 'time', string.format('%.20f', now))

  redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

  if redis.call('zscore', 'ql:tracked', self.jid) ~= false then
    Qless.publish('completed', self.jid)
  end

  if nextq then
    queue_obj = Qless.queue(nextq)
    Qless.publish(
      'log',
      cjson.encode({
        jid = self.jid,
        event = 'advanced',
        queue = queue,
        to = nextq,
      })
    )

    self:history(now, 'put', { q = nextq })

    if redis.call('zscore', 'ql:queues', nextq) == false then
      redis.call('zadd', 'ql:queues', now, nextq)
    end

    hmset(QlessJob.ns .. self.jid, {
      state = 'waiting',
      worker = '',
      failure = '{}',
      queue = nextq,
      expires = 0,
      remaining = tonumber(retries),
    })

    if (delay > 0) and (#depends == 0) then
      queue_obj.scheduled.add(now + delay, self.jid)
      return 'scheduled'
    else
      local count = 0
      for _, j in ipairs(depends) do
        local dep_state = redis.call('hget', QlessJob.ns .. j, 'state')
        if dep_state and dep_state ~= 'complete' then
          count = count + 1
          redis.call('sadd', QlessJob.ns .. j .. '-dependents', self.jid)
          redis.call('sadd', QlessJob.ns .. self.jid .. '-dependencies', j)
        end
      end
      if count > 0 then
        queue_obj.depends.add(now, self.jid)
        redis.call('hset', QlessJob.ns .. self.jid, 'state', 'depends')
        if delay > 0 then
          queue_obj.depends.add(now, self.jid)
          redis.call('hset', QlessJob.ns .. self.jid, 'scheduled', now + delay)
        end
        return 'depends'
      else
        if self:acquire_resources(now) then
          queue_obj.work.add(now, priority, self.jid)
        end
        return 'waiting'
      end
    end
  else
    Qless.publish(
      'log',
      cjson.encode({
        jid = self.jid,
        event = 'completed',
        queue = queue,
      })
    )

    hmset(QlessJob.ns .. self.jid, {
      state = 'complete',
      worker = '',
      failure = '{}',
      queue = '',
      expires = 0,
      remaining = tonumber(retries),
      throttle_next_run = next_run,
    })

    local count = Qless.config.get('jobs-history-count')
    local history_time = Qless.config.get('jobs-history')

    count = tonumber(count or 50000)
    history_time = tonumber(history_time or 7 * 24 * 60 * 60)

    redis.call('zadd', 'ql:completed', now, self.jid)

    local jids = redis.call('zrangebyscore', 'ql:completed', 0, now - history_time)
    for _, jid in ipairs(jids) do
      local tags = cjson.decode(redis.call('hget', QlessJob.ns .. jid, 'tags') or '{}')
      for _, tag in ipairs(tags) do
        redis.call('zrem', 'ql:t:' .. tag, jid)
        redis.call('zincrby', 'ql:tags', -1, tag)
      end
      redis.call('del', QlessJob.ns .. jid)
      redis.call('del', QlessJob.ns .. jid .. '-history')
    end
    redis.call('zremrangebyscore', 'ql:completed', 0, now - history_time)

    jids = redis.call('zrange', 'ql:completed', 0, (-1 - count))
    for _, jid in ipairs(jids) do
      local tags = cjson.decode(redis.call('hget', QlessJob.ns .. jid, 'tags') or '{}')
      for _, tag in ipairs(tags) do
        redis.call('zrem', 'ql:t:' .. tag, jid)
        redis.call('zincrby', 'ql:tags', -1, tag)
      end
      redis.call('del', QlessJob.ns .. jid)
      redis.call('del', QlessJob.ns .. jid .. '-history')
    end
    redis.call('zremrangebyrank', 'ql:completed', 0, (-1 - count))

    for _, j in ipairs(redis.call('smembers', QlessJob.ns .. self.jid .. '-dependents')) do
      redis.call('srem', QlessJob.ns .. j .. '-dependencies', self.jid)
      if redis.call('scard', QlessJob.ns .. j .. '-dependencies') == 0 then
        local q, p, scheduled = unpack(redis.call('hmget', QlessJob.ns .. j, 'queue', 'priority', 'scheduled'))
        if q then
          local dep_queue_obj = Qless.queue(q)
          dep_queue_obj.depends.remove(j)
          if scheduled then
            dep_queue_obj.scheduled.add(scheduled, j)
            redis.call('hset', QlessJob.ns .. j, 'state', 'scheduled')
            redis.call('hdel', QlessJob.ns .. j, 'scheduled')
          else
            if Qless.job(j):acquire_resources(now) then
              dep_queue_obj.work.add(now, p, j)
            end
            redis.call('hset', QlessJob.ns .. j, 'state', 'waiting')
          end
        end
      end
    end

    redis.call('del', QlessJob.ns .. self.jid .. '-dependents')

    return 'complete'
  end
end

function QlessJob:fail(now, worker, group, message, data)
  assert(worker, 'Fail(): Arg "worker" missing')
  assert(group, 'Fail(): Arg "group" missing')
  assert(message, 'Fail(): Arg "message" missing')

  local bin = now - (now % 86400)

  if data then
    assert(cjson.decode(data), 'Fail(): Arg "data" not JSON: ' .. tostring(data))
  end

  local queue, state, oldworker = unpack(redis.call('hmget', QlessJob.ns .. self.jid, 'queue', 'state', 'worker'))

  if not state then
    error('Fail(): Job ' .. self.jid .. ' does not exist')
  elseif state ~= 'running' then
    error('Fail(): Job ' .. self.jid .. ' not currently running: ' .. state)
  elseif worker ~= oldworker then
    error('Fail(): Job ' .. self.jid .. ' running with another worker: ' .. oldworker)
  end

  Qless.publish(
    'log',
    cjson.encode({
      jid = self.jid,
      event = 'failed',
      worker = worker,
      group = group,
      message = message,
    })
  )

  if redis.call('zscore', 'ql:tracked', self.jid) ~= false then
    Qless.publish('failed', self.jid)
  end

  redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

  self:history(now, 'failed', { worker = worker, group = group })

  redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failures', 1)
  redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed', 1)

  local queue_obj = Qless.queue(queue)
  queue_obj.work.remove(self.jid)
  queue_obj.locks.remove(self.jid)
  queue_obj.scheduled.remove(self.jid)

  self:release_resources(now)

  if data then
    redis.call('hset', QlessJob.ns .. self.jid, 'data', data)
  end

  hmset(QlessJob.ns .. self.jid, {
    state = 'failed',
    worker = '',
    expires = '',
    failure = cjson.encode({
      ['group'] = group,
      ['message'] = message,
      ['when'] = math.floor(now),
      ['worker'] = worker,
    }),
  })

  redis.call('sadd', 'ql:failures', group)
  redis.call('lpush', 'ql:f:' .. group, self.jid)


  return self.jid
end

function QlessJob:retry(now, queue, worker, delay, group, message)
  assert(queue, 'Retry(): Arg "queue" missing')
  assert(worker, 'Retry(): Arg "worker" missing')
  delay = assert(tonumber(delay or 0), 'Retry(): Arg "delay" not a number: ' .. tostring(delay))

  local oldqueue, state, oldworker, priority =
    unpack(redis.call('hmget', QlessJob.ns .. self.jid, 'queue', 'state', 'worker', 'priority'))

  if oldworker == false then
    error('Retry(): Job ' .. self.jid .. ' does not exist')
  elseif state ~= 'running' then
    error('Retry(): Job ' .. self.jid .. ' is not currently running: ' .. state)
  elseif oldworker ~= worker then
    error('Retry(): Job ' .. self.jid .. ' has been given to another worker: ' .. oldworker)
  end

  local remaining = tonumber(redis.call('hincrby', QlessJob.ns .. self.jid, 'remaining', -1)) or 0
  redis.call('hdel', QlessJob.ns .. self.jid, 'grace')

  Qless.queue(oldqueue).locks.remove(self.jid)
  self:release_resources(now)

  redis.call('zrem', 'ql:w:' .. worker .. ':jobs', self.jid)

  if remaining < 0 then
    group = group or ('failed-retries-' .. queue)
    self:history(now, 'failed', { ['group'] = group })

    redis.call('hmset', QlessJob.ns .. self.jid, 'state', 'failed', 'worker', '', 'expires', '')
    message = message or ('Job exhausted retries in queue "' .. oldqueue .. '"')
    redis.call(
      'hset',
      QlessJob.ns .. self.jid,
      'failure',
      cjson.encode({
        ['group'] = group,
        ['message'] = message,
        ['when'] = math.floor(now),
        ['worker'] = worker,
      })
    )

    if redis.call('zscore', 'ql:tracked', self.jid) ~= false then
      Qless.publish('failed', self.jid)
    end

    redis.call('sadd', 'ql:failures', group)
    redis.call('lpush', 'ql:f:' .. group, self.jid)
    local bin = now - (now % 86400)
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failures', 1)
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed', 1)
  else
    local queue_obj = Qless.queue(queue)
    if delay > 0 then
      queue_obj.scheduled.add(now + delay, self.jid)
      redis.call('hset', QlessJob.ns .. self.jid, 'state', 'scheduled')
    else
      if self:acquire_resources(now) then
        queue_obj.work.add(now, priority, self.jid)
      end
      redis.call('hset', QlessJob.ns .. self.jid, 'state', 'waiting')
    end

    if group ~= nil and message ~= nil then
      redis.call(
        'hset',
        QlessJob.ns .. self.jid,
        'failure',
        cjson.encode({
          ['group'] = group,
          ['message'] = message,
          ['when'] = math.floor(now),
          ['worker'] = worker,
        })
      )
    end
  end

  return math.floor(remaining)
end

function QlessJob:depends(now, command, ...)
  assert(command, 'Depends(): Arg "command" missing')
  local state = redis.call('hget', QlessJob.ns .. self.jid, 'state')
  if state ~= 'depends' then
    error('Depends(): Job ' .. self.jid .. ' not in the depends state: ' .. tostring(state))
  end

  if command == 'on' then
    for i = 1, select('#', ...) do
      local j = select(i, ...)
      local dep_state = redis.call('hget', QlessJob.ns .. j, 'state')
      if dep_state and dep_state ~= 'complete' then
        redis.call('sadd', QlessJob.ns .. j .. '-dependents', self.jid)
        redis.call('sadd', QlessJob.ns .. self.jid .. '-dependencies', j)
      end
    end
    return true
  elseif command == 'off' then
    if (...) == 'all' then
      for _, j in ipairs(redis.call('smembers', QlessJob.ns .. self.jid .. '-dependencies')) do
        redis.call('srem', QlessJob.ns .. j .. '-dependents', self.jid)
      end
      redis.call('del', QlessJob.ns .. self.jid .. '-dependencies')
      local q, p = unpack(redis.call('hmget', QlessJob.ns .. self.jid, 'queue', 'priority'))
      if q then
        local queue_obj = Qless.queue(q)
        queue_obj.depends.remove(self.jid)
        if self:acquire_resources(now) then
          queue_obj.work.add(now, p, self.jid)
        end
        redis.call('hset', QlessJob.ns .. self.jid, 'state', 'waiting')
      end
    else
      for i = 1, select('#', ...) do
        local j = select(i, ...)
        redis.call('srem', QlessJob.ns .. j .. '-dependents', self.jid)
        redis.call('srem', QlessJob.ns .. self.jid .. '-dependencies', j)
        if redis.call('scard', QlessJob.ns .. self.jid .. '-dependencies') == 0 then
          local q, p = unpack(redis.call('hmget', QlessJob.ns .. self.jid, 'queue', 'priority'))
          if q then
            local queue_obj = Qless.queue(q)
            queue_obj.depends.remove(self.jid)
            if self:acquire_resources(now) then
              queue_obj.work.add(now, p, self.jid)
            end
            redis.call('hset', QlessJob.ns .. self.jid, 'state', 'waiting')
          end
        end
      end
    end
    return true
  else
    error('Depends(): Argument "command" must be "on" or "off"')
  end
end

function QlessJob:heartbeat(now, worker, data)
  assert(worker, 'Heartbeat(): Arg "worker" missing')

  local queue = redis.call('hget', QlessJob.ns .. self.jid, 'queue') or ''
  local expires = now + tonumber(Qless.config.get(queue .. '-heartbeat') or Qless.config.get('heartbeat', 60))

  if data then
    assert(cjson.decode(data), 'Heartbeat(): Arg "data" not JSON: ' .. tostring(data))
  end

  local job_worker, state = unpack(redis.call('hmget', QlessJob.ns .. self.jid, 'worker', 'state'))
  if job_worker == false then
    error('Heartbeat(): Job ' .. self.jid .. ' does not exist')
  elseif state ~= 'running' then
    error('Heartbeat(): Job ' .. self.jid .. ' not currently running: ' .. state)
  elseif job_worker ~= worker or #job_worker == 0 then
    error('Heartbeat(): Job ' .. self.jid .. ' given out to another worker: ' .. job_worker)
  else
    if data then
      redis.call('hmset', QlessJob.ns .. self.jid, 'expires', expires, 'worker', worker, 'data', data)
    else
      redis.call('hmset', QlessJob.ns .. self.jid, 'expires', expires, 'worker', worker)
    end

    redis.call('zadd', 'ql:w:' .. worker .. ':jobs', expires, self.jid)

    redis.call('zadd', 'ql:workers', now, worker)

    local queue_obj = Qless.queue(redis.call('hget', QlessJob.ns .. self.jid, 'queue'))
    queue_obj.locks.add(expires, self.jid)
    return expires
  end
end

function QlessJob:priority(priority)
  priority = assert(tonumber(priority), 'Priority(): Arg "priority" missing or not a number: ' .. tostring(priority))

  local queue = redis.call('hget', QlessJob.ns .. self.jid, 'queue')

  if not queue then
    error('Priority(): Job ' .. self.jid .. ' does not exist')
  elseif queue == '' then
    redis.call('hset', QlessJob.ns .. self.jid, 'priority', priority)
    return priority
  else
    local queue_obj = Qless.queue(queue)
    if queue_obj.work.score(self.jid) then
      queue_obj.work.add(0, priority, self.jid)
    end
    redis.call('hset', QlessJob.ns .. self.jid, 'priority', priority)
    return priority
  end
end

function QlessJob:update(data)
  local tmp = {}
  for k, v in pairs(data) do
    table.insert(tmp, k)
    table.insert(tmp, v)
  end
  redis.call('hmset', QlessJob.ns .. self.jid, unpack(tmp))
end

function QlessJob:timeout(now)
  local queue_name, state, worker = unpack(redis.call('hmget', QlessJob.ns .. self.jid, 'queue', 'state', 'worker'))
  if not queue_name then
    error('Timeout(): Job ' .. self.jid .. ' does not exist')
  elseif state ~= 'running' then
    error('Timeout(): Job ' .. self.jid .. ' not running')
  else
    self:history(now, 'timed-out')
    local queue = Qless.queue(queue_name)
    queue.locks.remove(self.jid)
    queue.work.add(now, '+inf', self.jid)
    redis.call('hmset', QlessJob.ns .. self.jid, 'state', 'stalled', 'expires', 0)
    local encoded = cjson.encode({
      jid = self.jid,
      event = 'lock_lost',
      worker = worker,
    })
    Qless.publish('w:' .. worker, encoded)
    Qless.publish('log', encoded)
    return queue_name
  end
end

function QlessJob:exists()
  return redis.call('exists', QlessJob.ns .. self.jid) == 1
end

function QlessJob:history(now, what, item)
  local history = redis.call('hget', QlessJob.ns .. self.jid, 'history')
  if history then
    history = cjson.decode(history)
    for _, value in ipairs(history) do
      redis.call(
        'rpush',
        QlessJob.ns .. self.jid .. '-history',
        cjson.encode({ math.floor(value.put), 'put', { q = value.q } })
      )

      if value.popped then
        redis.call(
          'rpush',
          QlessJob.ns .. self.jid .. '-history',
          cjson.encode({ math.floor(value.popped), 'popped', { worker = value.worker } })
        )
      end

      if value.failed then
        redis.call(
          'rpush',
          QlessJob.ns .. self.jid .. '-history',
          cjson.encode({ math.floor(value.failed), 'failed', nil })
        )
      end

      if value.done then
        redis.call(
          'rpush',
          QlessJob.ns .. self.jid .. '-history',
          cjson.encode({ math.floor(value.done), 'done', nil })
        )
      end
    end
    redis.call('hdel', QlessJob.ns .. self.jid, 'history')
  end

  if what == nil then
    local response = {}
    for _, value in ipairs(redis.call('lrange', QlessJob.ns .. self.jid .. '-history', 0, -1)) do
      value = cjson.decode(value)
      local dict = value[3] or {}
      dict['when'] = value[1]
      dict['what'] = value[2]
      table.insert(response, dict)
    end
    return response
  else
    local count = tonumber(Qless.config.get('max-job-history', 100))
    if count > 0 then
      local obj = redis.call('lpop', QlessJob.ns .. self.jid .. '-history')
      redis.call('ltrim', QlessJob.ns .. self.jid .. '-history', -count + 2, -1)
      if obj ~= nil and obj ~= false then
        redis.call('lpush', QlessJob.ns .. self.jid .. '-history', obj)
      end
    end
    return redis.call('rpush', QlessJob.ns .. self.jid .. '-history', cjson.encode({ math.floor(now), what, item }))
  end
end

function QlessJob:release_resources(now)
  local resources = redis.call('hget', QlessJob.ns .. self.jid, 'resources')
  resources = cjson.decode(resources or '[]')
  for _, res in ipairs(resources) do
    Qless.resource(res):release(now, self.jid)
  end
end

function QlessJob:acquire_resources(now)
  local resources, priority = unpack(redis.call('hmget', QlessJob.ns .. self.jid, 'resources', 'priority'))
  resources = cjson.decode(resources or '[]')
  if #resources == 0 then
    return true
  end

  local acquired_all = true

  for _, rid in ipairs(resources) do
    local ok, res = pcall(function()
      return Qless.resource(rid):acquire(now, priority, self.jid)
    end)
    if not ok then
      self:set_failed(now, 'system:fatal', res.msg)
      return false
    end
    acquired_all = acquired_all and res
  end

  return acquired_all
end

function QlessJob:set_failed(now, group, message, worker, release_work, release_resources)
  assert(group, 'Fail(): Arg "group" missing')
  assert(message, 'Fail(): Arg "message" missing')
  worker = worker or 'none'
  release_work = release_work or true
  release_resources = release_resources or false

  local bin = now - (now % 86400)

  local queue = unpack(redis.call('hmget', QlessJob.ns .. self.jid, 'queue'))

  Qless.publish(
    'log',
    cjson.encode({
      jid = self.jid,
      event = 'failed',
      worker = worker,
      group = group,
      message = message,
    })
  )

  if redis.call('zscore', 'ql:tracked', self.jid) ~= false then
    Qless.publish('failed', self.jid)
  end

  self:history(now, 'failed', { group = group })

  redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failures', 1)
  redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. queue, 'failed', 1)

  if release_work then
    local queue_obj = Qless.queue(queue)
    queue_obj.work.remove(self.jid)
    queue_obj.locks.remove(self.jid)
    queue_obj.scheduled.remove(self.jid)
  end

  if release_resources then
    self:release_resources(now)
  end

  hmset(QlessJob.ns .. self.jid, {
    state = 'failed',
    worker = '',
    expires = '',
    failure = cjson.encode({
      ['group'] = group,
      ['message'] = message,
      ['when'] = math.floor(now),
    }),
  })

  redis.call('sadd', 'ql:failures', group)
  redis.call('lpush', 'ql:f:' .. group, self.jid)


  return self.jid
end
function Qless.queue(name)
  assert(name, 'Queue(): no queue name provided')
  local queue = {}
  setmetatable(queue, QlessQueue)
  queue.name = name

  queue.work = {
    peek = function(now, offset, count)
      if count == 0 then
        return {}
      end
      local jids = {}
      for _, jid in ipairs(redis.call('zrevrange', queue:prefix('work'), offset, offset + count - 1)) do
        table.insert(jids, jid)
      end
      return jids
    end,
    remove = function(...)
      if select('#', ...) > 0 then
        return redis.call('zrem', queue:prefix('work'), ...)
      end
    end,
    add = function(now, priority, jid)
      if priority ~= '+inf' then
        priority = priority - (now / 10000000000)
      end
      return redis.call('zadd', queue:prefix('work'), priority, jid)
    end,
    score = function(jid)
      return redis.call('zscore', queue:prefix('work'), jid)
    end,
    length = function()
      return redis.call('zcard', queue:prefix('work'))
    end,
  }

  queue.locks = {
    expired = function(now, offset, count)
      return redis.call('zrangebyscore', queue:prefix('locks'), '-inf', now, 'LIMIT', offset, count)
    end,
    peek = function(now, offset, count)
      return redis.call('zrangebyscore', queue:prefix('locks'), now, '+inf', 'LIMIT', offset, count)
    end,
    add = function(expires, jid)
      redis.call('zadd', queue:prefix('locks'), expires, jid)
    end,
    remove = function(...)
      if select('#', ...) > 0 then
        return redis.call('zrem', queue:prefix('locks'), ...)
      end
    end,
    running = function(now)
      return redis.call('zcount', queue:prefix('locks'), now, '+inf')
    end,
    length = function(now)
      if now then
        return redis.call('zcount', queue:prefix('locks'), 0, now)
      else
        return redis.call('zcard', queue:prefix('locks'))
      end
    end,
    job_time_left = function(now, jid)
      return tonumber(redis.call('zscore', queue:prefix('locks'), jid) or 0) - now
    end,
  }

  queue.depends = {
    peek = function(now, offset, count)
      return redis.call('zrange', queue:prefix('depends'), offset, offset + count - 1)
    end,
    add = function(now, jid)
      redis.call('zadd', queue:prefix('depends'), now, jid)
    end,
    remove = function(...)
      if select('#', ...) > 0 then
        return redis.call('zrem', queue:prefix('depends'), ...)
      end
    end,
    length = function()
      return redis.call('zcard', queue:prefix('depends'))
    end,
  }

  queue.scheduled = {
    peek = function(now, offset, count)
      return redis.call('zrange', queue:prefix('scheduled'), offset, offset + count - 1)
    end,
    ready = function(now, offset, count)
      return redis.call('zrangebyscore', queue:prefix('scheduled'), 0, now, 'LIMIT', offset, count)
    end,
    add = function(when, jid)
      redis.call('zadd', queue:prefix('scheduled'), when, jid)
    end,
    remove = function(...)
      if select('#', ...) > 0 then
        return redis.call('zrem', queue:prefix('scheduled'), ...)
      end
    end,
    length = function()
      return redis.call('zcard', queue:prefix('scheduled'))
    end,
  }

  queue.recurring = {
    peek = function(now, offset, count)
      return redis.call('zrangebyscore', queue:prefix('recur'), 0, now, 'LIMIT', offset, count)
    end,
    add = function(when, jid)
      redis.call('zadd', queue:prefix('recur'), when, jid)
    end,
    remove = function(...)
      if select('#', ...) > 0 then
        return redis.call('zrem', queue:prefix('recur'), ...)
      end
    end,
    update = function(increment, jid)
      redis.call('zincrby', queue:prefix('recur'), increment, jid)
    end,
    score = function(jid)
      return redis.call('zscore', queue:prefix('recur'), jid)
    end,
    length = function()
      return redis.call('zcard', queue:prefix('recur'))
    end,
  }
  return queue
end

function QlessQueue:prefix(group)
  return QlessQueue.ns .. self.name .. (group and ('-' .. group) or '')
end

function QlessQueue:stats(now, date)
  date = assert(tonumber(date), 'Stats(): Arg "date" missing or not a number: ' .. (date or 'nil'))

  local bin = date - (date % 86400)

  local histokeys = {}
  for _, unit in ipairs({ { 's', 0, 59 }, { 'm', 1, 59 }, { 'h', 1, 23 }, { 'd', 1, 6 } }) do
    for i = unit[2], unit[3] do
      table.insert(histokeys, unit[1] .. i)
    end
  end

  local mkstats = function(name, queue)
    local results = {}

    local key = 'ql:s:' .. name .. ':' .. bin .. ':' .. queue
    local count, mean, vk = unpack(redis.call('hmget', key, 'total', 'mean', 'vk'))

    count = tonumber(count) or 0
    mean = tonumber(mean) or 0
    vk = tonumber(vk)

    results.count = count or 0
    results.mean = mean or 0
    results.histogram = {}

    if not count then
      results.std = 0
    else
      if count > 1 then
        results.std = math.sqrt(vk / (count - 1))
      else
        results.std = 0
      end
    end

    local histogram = redis.call('hmget', key, unpack(histokeys))
    for i = 1, #histokeys do
      table.insert(results.histogram, tonumber(histogram[i]) or 0)
    end
    return results
  end

  local retries, failed, failures =
    unpack(redis.call('hmget', 'ql:s:stats:' .. bin .. ':' .. self.name, 'retries', 'failed', 'failures'))
  return {
    retries = tonumber(retries or 0),
    failed = tonumber(failed or 0),
    failures = tonumber(failures or 0),
    wait = mkstats('wait', self.name),
    run = mkstats('run', self.name),
  }
end

function QlessQueue:peek(now, count)
  count = assert(tonumber(count), 'Peek(): Arg "count" missing or not a number: ' .. tostring(count))

  local jids = self.locks.expired(now, 0, count)

  self:check_recurring(now, count - #jids)

  self:check_scheduled(now, count - #jids)

  extend_table(jids, self.work.peek(now, 0, count - #jids))

  return jids
end

function QlessQueue:paused()
  return redis.call('sismember', 'ql:paused_queues', self.name) == 1
end

function QlessQueue.pause(now, ...)
  redis.call('sadd', 'ql:paused_queues', ...)
end

function QlessQueue.unpause(...)
  redis.call('srem', 'ql:paused_queues', ...)
end

function QlessQueue:pop(now, worker, count)
  assert(worker, 'Pop(): Arg "worker" missing')
  count = assert(tonumber(count), 'Pop(): Arg "count" missing or not a number: ' .. tostring(count))

  local expires = now + tonumber(Qless.config.get(self.name .. '-heartbeat') or Qless.config.get('heartbeat', 60))

  if self:paused() then
    return {}
  end

  redis.call('zadd', 'ql:workers', now, worker)

  local max_concurrency = tonumber(Qless.config.get(self.name .. '-max-concurrency', 0))

  if max_concurrency > 0 then
    local allowed = math.max(0, max_concurrency - self.locks.running(now))
    count = math.min(allowed, count)
    if count == 0 then
      return {}
    end
  end

  local jids = self:invalidate_locks(now, count)

  self:check_recurring(now, count - #jids)

  self:check_scheduled(now, count - #jids)

  extend_table(jids, self.work.peek(now, 0, count - #jids))

  for _, jid in ipairs(jids) do
    local job = Qless.job(jid)
    job:history(now, 'popped', { worker = worker })

    local time = tonumber(redis.call('hget', QlessJob.ns .. jid, 'time') or now)
    local waiting = now - time
    self:stat(now, 'wait', waiting)
    redis.call('hset', QlessJob.ns .. jid, 'time', string.format('%.20f', now))

    redis.call('zadd', 'ql:w:' .. worker .. ':jobs', expires, jid)

    job:update({
      worker = worker,
      expires = expires,
      state = 'running',
    })

    self.locks.add(expires, jid)

    local tracked = redis.call('zscore', 'ql:tracked', jid) ~= false
    if tracked then
      Qless.publish('popped', jid)
    end
  end

  self.work.remove(unpack(jids))

  return jids
end

function QlessQueue:stat(now, stat, val)
  local bin = now - (now % 86400)
  local key = 'ql:s:' .. stat .. ':' .. bin .. ':' .. self.name

  local count, mean, vk = unpack(redis.call('hmget', key, 'total', 'mean', 'vk'))

  count = count or 0
  if count == 0 then
    mean = val
    vk = 0
    count = 1
  else
    count = count + 1
    local oldmean = mean
    mean = mean + (val - mean) / count
    vk = vk + (val - mean) * (val - oldmean)
  end

  val = math.floor(val)
  if val < 60 then -- seconds
    redis.call('hincrby', key, 's' .. val, 1)
  elseif val < 3600 then -- minutes
    redis.call('hincrby', key, 'm' .. math.floor(val / 60), 1)
  elseif val < 86400 then -- hours
    redis.call('hincrby', key, 'h' .. math.floor(val / 3600), 1)
  else -- days
    redis.call('hincrby', key, 'd' .. math.floor(val / 86400), 1)
  end
  redis.call('hmset', key, 'total', count, 'mean', mean, 'vk', vk)
end

function QlessQueue:put(now, worker, jid, klass, data, delay, ...)
  assert(jid, 'Put(): Arg "jid" missing')
  assert(klass, 'Put(): Arg "klass" missing')
  assert(cjson.decode(data), 'Put(): Arg "data" missing or not JSON: ' .. tostring(data))
  delay = assert(tonumber(delay), 'Put(): Arg "delay" not a number: ' .. tostring(delay))

  local arg_count = select('#', ...)
  if arg_count % 2 == 1 then
    error('Odd number of additional args')
  end
  local options = {}
  for i = 1, arg_count, 2 do
    local key, val = select(i, ...)
    options[key] = val
  end

  local job = Qless.job(jid)
  local priority, tags, oldqueue, state, failure, retries, oldworker, interval, next_run, old_resources = unpack(
    redis.call(
      'hmget',
      QlessJob.ns .. jid,
      'priority',
      'tags',
      'queue',
      'state',
      'failure',
      'retries',
      'worker',
      'throttle_interval',
      'throttle_next_run',
      'resources'
    )
  )

  next_run = next_run or now

  local replace =
    assert(tonumber(options['replace'] or 1), 'Put(): Arg "replace" not a number: ' .. tostring(options['replace']))

  if replace == 0 and state == 'running' then
    local time_left = self.locks.job_time_left(now, jid)
    if time_left > 0 then
      return time_left
    end
  end

  if tags then
    Qless.tag(now, 'remove', jid, unpack(cjson.decode(tags)))
  end

  retries = assert(
    tonumber(options['retries'] or retries or 5),
    'Put(): Arg "retries" not a number: ' .. tostring(options['retries'])
  )
  tags =
    assert(cjson.decode(options['tags'] or tags or '[]'), 'Put(): Arg "tags" not JSON' .. tostring(options['tags']))
  priority = assert(
    tonumber(options['priority'] or priority or 0),
    'Put(): Arg "priority" not a number' .. tostring(options['priority'])
  )
  local depends =
    assert(cjson.decode(options['depends'] or '[]'), 'Put(): Arg "depends" not JSON: ' .. tostring(options['depends']))

  local resources = assert(
    cjson.decode(options['resources'] or '[]'),
    'Put(): Arg "resources" not JSON array: ' .. tostring(options['resources'])
  )
  assert(#resources == 0 or QlessResource.all_exist(resources), 'Put(): invalid resources requested')

  if old_resources then
    old_resources = Set.new(cjson.decode(old_resources))
    local removed_resources = Set.diff(old_resources, Set.new(resources))
    for k in pairs(removed_resources) do
      Qless.resource(k):release(now, jid)
    end
  end

  interval = assert(
    tonumber(options['interval'] or interval or 0),
    'Put(): Arg "interval" not a number: ' .. tostring(options['interval'])
  )

  if interval > 0 then
    local minimum_delay = next_run - now
    if minimum_delay >= 0 then
      delay = math.max(delay, minimum_delay)
    end
  else
    next_run = 0
  end

  if #depends > 0 then
    local new = {}
    for _, d in ipairs(depends) do
      new[d] = 1
    end

    local original = redis.call('smembers', QlessJob.ns .. jid .. '-dependencies')
    for _, dep in pairs(original) do
      if not new[dep] then
        redis.call('srem', QlessJob.ns .. dep .. '-dependents', jid)
        redis.call('srem', QlessJob.ns .. jid .. '-dependencies', dep)
      end
    end
  end

  Qless.publish(
    'log',
    cjson.encode({
      jid = jid,
      event = 'put',
      queue = self.name,
    })
  )

  job:history(now, 'put', { q = self.name })

  if oldqueue then
    local queue_obj = Qless.queue(oldqueue)
    queue_obj.work.remove(jid)
    queue_obj.locks.remove(jid)
    queue_obj.depends.remove(jid)
    queue_obj.scheduled.remove(jid)
  end

  if oldworker and oldworker ~= '' then
    redis.call('zrem', 'ql:w:' .. oldworker .. ':jobs', jid)
    if oldworker ~= worker then
      local encoded = cjson.encode({
        jid = jid,
        event = 'lock_lost',
        worker = oldworker,
      })
      Qless.publish('w:' .. oldworker, encoded)
      Qless.publish('log', encoded)
    end
  end

  if state == 'complete' then
    redis.call('zrem', 'ql:completed', jid)
  end

  for _, tag in ipairs(tags) do
    redis.call('zadd', 'ql:t:' .. tag, now, jid)
    redis.call('zincrby', 'ql:tags', 1, tag)
  end

  if state == 'failed' then
    failure = cjson.decode(failure)
    redis.call('lrem', 'ql:f:' .. failure.group, 0, jid)
    if redis.call('llen', 'ql:f:' .. failure.group) == 0 then
      redis.call('srem', 'ql:failures', failure.group)
    end
    local bin = failure.when - (failure.when % 86400)
    redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. self.name, 'failed', -1)
  end

  hmset(QlessJob.ns .. jid, {
    jid = jid,
    klass = klass,
    data = data,
    priority = priority,
    tags = cjson.encode(tags),
    resources = cjson.encode(resources),
    state = ((delay > 0) and 'scheduled') or 'waiting',
    worker = '',
    expires = 0,
    queue = self.name,
    retries = retries,
    remaining = retries,
    time = string.format('%.20f', now),
    throttle_interval = interval,
    throttle_next_run = next_run,
    result_data = '{}',
  })

  for _, j in ipairs(depends) do
    local dep_state = redis.call('hget', QlessJob.ns .. j, 'state')
    if dep_state and dep_state ~= 'complete' then
      redis.call('sadd', QlessJob.ns .. j .. '-dependents', jid)
      redis.call('sadd', QlessJob.ns .. jid .. '-dependencies', j)
    end
  end

  if delay > 0 then
    if redis.call('scard', QlessJob.ns .. jid .. '-dependencies') > 0 then
      self.depends.add(now, jid)
      redis.call('hmset', QlessJob.ns .. jid, 'state', 'depends', 'scheduled', now + delay)
    else
      self.scheduled.add(now + delay, jid)
    end
  else
    if redis.call('scard', QlessJob.ns .. jid .. '-dependencies') > 0 then
      self.depends.add(now, jid)
      redis.call('hset', QlessJob.ns .. jid, 'state', 'depends')
    elseif #resources > 0 then
      if Qless.job(jid):acquire_resources(now) then
        self.work.add(now, priority, jid)
      end
    else
      self.work.add(now, priority, jid)
    end
  end

  if redis.call('zscore', 'ql:queues', self.name) == false then
    redis.call('zadd', 'ql:queues', now, self.name)
  end

  if redis.call('zscore', 'ql:tracked', jid) ~= false then
    Qless.publish('put', jid)
  end

  return jid
end

function QlessQueue:unfail(now, group, count)
  assert(group, 'Unfail(): Arg "group" missing')
  count = assert(tonumber(count or 25), 'Unfail(): Arg "count" not a number: ' .. tostring(count))

  local jids = redis.call('lrange', 'ql:f:' .. group, -count, -1)

  for _, jid in ipairs(jids) do
    local job = Qless.job(jid)
    local data = job:data()
    job:history(now, 'put', { q = self.name })
    hmset(QlessJob.ns .. data.jid, {
      state = 'waiting',
      worker = '',
      expires = 0,
      queue = self.name,
      remaining = data.retries or 5,
    })

    if job:acquire_resources(now) then
      self.work.add(now, data.priority, data.jid)
    end
  end

  redis.call('ltrim', 'ql:f:' .. group, 0, -count - 1)
  if redis.call('llen', 'ql:f:' .. group) == 0 then
    redis.call('srem', 'ql:failures', group)
  end

  return #jids
end

function QlessQueue:recur(now, jid, klass, data, spec, ...)
  assert(jid, 'RecurringJob On(): Arg "jid" missing')
  assert(klass, 'RecurringJob On(): Arg "klass" missing')
  assert(spec, 'RecurringJob On(): Arg "spec" missing')
  assert(cjson.decode(data), 'RecurringJob On(): Arg "data" missing or not JSON: ' .. tostring(data))

  if spec == 'interval' then
    local interval, offset = ...
    interval = assert(tonumber(interval), 'Recur(): Arg "interval" not a number: ' .. tostring(interval))
    offset = assert(tonumber(offset), 'Recur(): Arg "offset" not a number: ' .. tostring(offset))
    if interval <= 0 then
      error('Recur(): Arg "interval" must be greater than 0')
    end

    local arg_count = select('#', ...)
    if arg_count % 2 == 1 then
      error('Odd number of additional args')
    end
    local options = {}
    for i = 3, arg_count, 2 do
      local key, val = select(i, ...)
      options[key] = val
    end
    options.tags = assert(
      cjson.decode(options.tags or '{}'),
      'Recur(): Arg "tags" must be JSON string array: ' .. tostring(options.tags)
    )
    options.priority =
      assert(tonumber(options.priority or 0), 'Recur(): Arg "priority" not a number: ' .. tostring(options.priority))
    options.retries =
      assert(tonumber(options.retries or 0), 'Recur(): Arg "retries" not a number: ' .. tostring(options.retries))
    options.backlog =
      assert(tonumber(options.backlog or 0), 'Recur(): Arg "backlog" not a number: ' .. tostring(options.backlog))
    options.resources = assert(
      cjson.decode(options['resources'] or '[]'),
      'Recur(): Arg "resources" not JSON array: ' .. tostring(options['resources'])
    )

    local count, old_queue = unpack(redis.call('hmget', 'ql:r:' .. jid, 'count', 'queue'))
    count = count or 0

    if old_queue then
      Qless.queue(old_queue).recurring.remove(jid)
    end

    hmset('ql:r:' .. jid, {
      jid = jid,
      klass = klass,
      data = data,
      priority = options.priority,
      tags = cjson.encode(options.tags or {}),
      state = 'recur',
      queue = self.name,
      type = 'interval',
      count = count,
      interval = interval,
      retries = options.retries,
      backlog = options.backlog,
      resources = cjson.encode(options.resources),
    })
    self.recurring.add(now + offset, jid)

    if redis.call('zscore', 'ql:queues', self.name) == false then
      redis.call('zadd', 'ql:queues', now, self.name)
    end

    return jid
  else
    error('Recur(): schedule type "' .. tostring(spec) .. '" unknown')
  end
end

function QlessQueue:length()
  return self.locks.length() + self.work.length() + self.scheduled.length()
end

function QlessQueue:check_recurring(now, count)
  local moved = 0
  local r = self.recurring.peek(now, 0, count)
  for _, jid in ipairs(r) do
    local klass, data, priority, tags, retries, interval, backlog, resources = unpack(
      redis.call(
        'hmget',
        'ql:r:' .. jid,
        'klass',
        'data',
        'priority',
        'tags',
        'retries',
        'interval',
        'backlog',
        'resources'
      )
    )
    local _tags = cjson.decode(tags)
    resources = cjson.decode(resources or '[]')
    local score = math.floor(tonumber(self.recurring.score(jid)) or 0)
    interval = tonumber(interval)

    backlog = tonumber(backlog or 0)
    if backlog ~= 0 then
      local num = ((now - score) / interval)
      if num > backlog then
        score = score + (math.ceil(num - backlog) * interval)
      end
    end

    while (score <= now) and (moved < count) do
      local spawn_count = redis.call('hincrby', 'ql:r:' .. jid, 'count', 1)
      moved = moved + 1

      local child_jid = jid .. '-' .. spawn_count

      for _, tag in ipairs(_tags) do
        redis.call('zadd', 'ql:t:' .. tag, now, child_jid)
        redis.call('zincrby', 'ql:tags', 1, tag)
      end

      hmset(QlessJob.ns .. child_jid, {
        jid = child_jid,
        klass = klass,
        data = data,
        priority = priority,
        tags = tags,
        state = 'waiting',
        worker = '',
        expires = 0,
        queue = self.name,
        retries = retries,
        remaining = retries,
        resources = cjson.encode(resources),
        throttle_interval = 0,
        time = string.format('%.20f', score),
        spawned_from_jid = jid,
      })

      local job = Qless.job(child_jid)
      job:history(score, 'put', { q = self.name })


      local add_job = true
      if #resources then
        add_job = job:acquire_resources(score)
      end
      if add_job then
        self.work.add(score, priority, child_jid)
      end

      score = score + interval
      self.recurring.add(score, jid)
    end
  end
end

function QlessQueue:check_scheduled(now, count)
  local scheduled = self.scheduled.ready(now, 0, count)
  for _, jid in ipairs(scheduled) do
    local priority = tonumber(redis.call('hget', QlessJob.ns .. jid, 'priority') or 0)
    if Qless.job(jid):acquire_resources(now) then
      self.work.add(now, priority, jid)
    end
    self.scheduled.remove(jid)

    redis.call('hset', QlessJob.ns .. jid, 'state', 'waiting')
  end
end

function QlessQueue:invalidate_locks(now, count)
  local jids = {}
  for _, jid in ipairs(self.locks.expired(now, 0, count)) do
    local worker = unpack(redis.call('hmget', QlessJob.ns .. jid, 'worker'))
    redis.call('zrem', 'ql:w:' .. worker .. ':jobs', jid)

    local grace_period = tonumber(Qless.config.get('grace-period'))

    local courtesy_sent = tonumber(redis.call('hget', QlessJob.ns .. jid, 'grace') or 0)

    local send_message = (courtesy_sent ~= 1)
    local invalidate = not send_message

    if grace_period <= 0 then
      send_message = true
      invalidate = true
    end

    if send_message then
      if redis.call('zscore', 'ql:tracked', jid) ~= false then
        Qless.publish('stalled', jid)
      end
      Qless.job(jid):history(now, 'timed-out')
      redis.call('hset', QlessJob.ns .. jid, 'grace', 1)

      local encoded = cjson.encode({
        jid = jid,
        event = 'lock_lost',
        worker = worker,
      })
      Qless.publish('w:' .. worker, encoded)
      Qless.publish('log', encoded)
      self.locks.add(now + grace_period, jid)

      local bin = now - (now % 86400)
      redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. self.name, 'retries', 1)
    end

    if invalidate then
      redis.call('hdel', QlessJob.ns .. jid, 'grace', 0)

      local remaining = tonumber(redis.call('hincrby', QlessJob.ns .. jid, 'remaining', -1))

      if remaining < 0 then
        Qless.job(jid):release_resources(now)

        self.work.remove(jid)
        self.locks.remove(jid)
        self.scheduled.remove(jid)

        local group = 'failed-retries-' .. self.name
        local job = Qless.job(jid)
        job:history(now, 'failed', { group = group })
        redis.call('hmset', QlessJob.ns .. jid, 'state', 'failed', 'worker', '', 'expires', '')
        redis.call(
          'hset',
          QlessJob.ns .. jid,
          'failure',
          cjson.encode({
            ['group'] = group,
            ['message'] = 'Job exhausted retries in queue "' .. self.name .. '"',
            ['when'] = now,
            ['worker'] = worker,
          })
        )

        redis.call('sadd', 'ql:failures', group)
        redis.call('lpush', 'ql:f:' .. group, jid)

        if redis.call('zscore', 'ql:tracked', jid) ~= false then
          Qless.publish('failed', jid)
        end
        Qless.publish(
          'log',
          cjson.encode({
            jid = jid,
            event = 'failed',
            group = group,
            worker = worker,
            message = 'Job exhausted retries in queue "' .. self.name .. '"',
          })
        )

        local bin = now - (now % 86400)
        redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. self.name, 'failures', 1)
        redis.call('hincrby', 'ql:s:stats:' .. bin .. ':' .. self.name, 'failed', 1)
      else
        table.insert(jids, jid)
      end
    end
  end

  return jids
end

function QlessQueue.deregister(...)
  redis.call('zrem', Qless.ns .. 'queues', ...)
end

function QlessQueue.counts(now, name)
  if name then
    local queue = Qless.queue(name)
    local stalled = queue.locks.length(now)
    queue:check_scheduled(now, queue.scheduled.length())
    return {
      name = name,
      waiting = queue.work.length(),
      stalled = stalled,
      running = queue.locks.length() - stalled,
      scheduled = queue.scheduled.length(),
      depends = queue.depends.length(),
      recurring = queue.recurring.length(),
      paused = queue:paused(),
    }
  else
    local queues = redis.call('zrange', 'ql:queues', 0, -1)
    local response = {}
    for _, qname in ipairs(queues) do
      table.insert(response, QlessQueue.counts(now, qname))
    end
    return response
  end
end
function QlessRecurringJob:data()
  local job = redis.call(
    'hmget',
    'ql:r:' .. self.jid,
    'jid',
    'klass',
    'state',
    'queue',
    'priority',
    'interval',
    'retries',
    'count',
    'data',
    'tags',
    'backlog'
  )

  if not job[1] then
    return nil
  end

  return {
    jid = job[1],
    klass = job[2],
    state = job[3],
    queue = job[4],
    priority = tonumber(job[5]),
    interval = tonumber(job[6]),
    retries = tonumber(job[7]),
    count = tonumber(job[8]),
    data = job[9],
    tags = cjson.decode(job[10]),
    backlog = tonumber(job[11] or 0),
  }
end

function QlessRecurringJob:update(now, ...)
  if redis.call('exists', 'ql:r:' .. self.jid) ~= 0 then
    local arg_count = select('#', ...)
    if arg_count % 2 == 1 then
      error('Odd number of additional args')
    end
    for i = 1, arg_count, 2 do
      local key, value = select(i, ...)
      assert(value, 'No value provided for ' .. tostring(key))
      if key == 'priority' or key == 'interval' or key == 'retries' then
        value = assert(tonumber(value), 'Recur(): Arg "' .. key .. '" must be a number: ' .. tostring(value))
        if key == 'interval' then
          local queue, interval = unpack(redis.call('hmget', 'ql:r:' .. self.jid, 'queue', 'interval'))
          Qless.queue(queue).recurring.update(value - tonumber(interval), self.jid)
        end
        redis.call('hset', 'ql:r:' .. self.jid, key, value)
      elseif key == 'data' then
        assert(cjson.decode(value), 'Recur(): Arg "data" is not JSON-encoded: ' .. tostring(value))
        redis.call('hset', 'ql:r:' .. self.jid, 'data', value)
      elseif key == 'klass' then
        redis.call('hset', 'ql:r:' .. self.jid, 'klass', value)
      elseif key == 'queue' then
        local queue_obj = Qless.queue(redis.call('hget', 'ql:r:' .. self.jid, 'queue'))
        local score = queue_obj.recurring.score(self.jid)
        queue_obj.recurring.remove(self.jid)
        Qless.queue(value).recurring.add(score, self.jid)
        redis.call('hset', 'ql:r:' .. self.jid, 'queue', value)
        if redis.call('zscore', 'ql:queues', value) == false then
          redis.call('zadd', 'ql:queues', now, value)
        end
      elseif key == 'backlog' then
        value = assert(tonumber(value), 'Recur(): Arg "backlog" not a number: ' .. tostring(value))
        redis.call('hset', 'ql:r:' .. self.jid, 'backlog', value)
      else
        error('Recur(): Unrecognized option "' .. key .. '"')
      end
    end
    return true
  else
    error('Recur(): No recurring job ' .. self.jid)
  end
end

function QlessRecurringJob:tag(...)
  local tags = redis.call('hget', 'ql:r:' .. self.jid, 'tags')
  if tags then
    tags = cjson.decode(tags)
    local _tags = {}
    for _, v in ipairs(tags) do
      _tags[v] = true
    end

    for i = 1, select('#', ...) do
      local tag = select(i, ...)
      if not _tags[tag] then
        table.insert(tags, tag)
      end
    end

    tags = cjson.encode(tags)
    redis.call('hset', 'ql:r:' .. self.jid, 'tags', tags)
    return tags
  else
    error('Tag(): Job ' .. self.jid .. ' does not exist')
  end
end

function QlessRecurringJob:untag(...)
  local tags = redis.call('hget', 'ql:r:' .. self.jid, 'tags')
  if tags then
    tags = cjson.decode(tags)
    local _tags = {}
    for _, v in ipairs(tags) do
      _tags[v] = true
    end
    for i = 1, select('#', ...) do
      _tags[select(i, ...)] = nil
    end
    local results = {}
    for _, tag in ipairs(tags) do
      if _tags[tag] then
        table.insert(results, tag)
      end
    end
    tags = cjson.encode(results)
    redis.call('hset', 'ql:r:' .. self.jid, 'tags', tags)
    return tags
  else
    error('Untag(): Job ' .. self.jid .. ' does not exist')
  end
end

function QlessRecurringJob:unrecur()
  local queue = redis.call('hget', 'ql:r:' .. self.jid, 'queue')
  if queue then
    Qless.queue(queue).recurring.remove(self.jid)
    redis.call('del', 'ql:r:' .. self.jid)
    return true
  else
    return true
  end
end
function QlessWorker.deregister(...)
  redis.call('zrem', 'ql:workers', ...)
end

function QlessWorker.counts(now, worker)
  local interval = tonumber(Qless.config.get('max-worker-age', 86400))

  local stale_workers = redis.call('zrangebyscore', 'ql:workers', 0, now - interval)
  for _, stale_worker in ipairs(stale_workers) do
    redis.call('del', 'ql:w:' .. stale_worker .. ':jobs')
  end

  redis.call('zremrangebyscore', 'ql:workers', 0, now - interval)

  if worker then
    return {
      jobs = redis.call('zrevrangebyscore', 'ql:w:' .. worker .. ':jobs', now + 8640000, now),
      stalled = redis.call('zrevrangebyscore', 'ql:w:' .. worker .. ':jobs', now, 0),
    }
  else
    local response = {}
    local workers = redis.call('zrevrange', 'ql:workers', 0, -1)
    for _, worker_name in ipairs(workers) do
      table.insert(response, {
        name = worker_name,
        jobs = redis.call('zcount', 'ql:w:' .. worker_name .. ':jobs', now, now + 8640000),
        stalled = redis.call('zcount', 'ql:w:' .. worker_name .. ':jobs', 0, now),
      })
    end
    return response
  end
end

function QlessResource:data()
  local res = redis.call('hmget', QlessResource.ns .. self.rid, 'rid', 'max')

  if not res[1] then
    return nil
  end

  local data = {
    rid = res[1],
    max = tonumber(res[2] or 0),
    pending = self:pending(),
    locks = self:locks(),
  }

  return data
end

function QlessResource:get()
  local res = redis.call('hmget', QlessResource.ns .. self.rid, 'rid', 'max')

  if not res[1] then
    return nil
  end

  return tonumber(res[2] or 0)
end

function QlessResource:set(now, max)
  max = assert(tonumber(max), 'Set(): Arg "max" not a number: ' .. tostring(max))

  local current_max = self:get()
  if current_max == nil then
    current_max = max
  end

  local key_locks = self:prefix('locks')
  local current_locks = redis.pcall('scard', key_locks)
  local confirm_limit = math.max(current_max, current_locks)
  local max_change = max - confirm_limit
  local key_pending = self:prefix('pending')

  redis.call('sadd', 'ql:resources', self.rid)
  redis.call('hmset', QlessResource.ns .. self.rid, 'rid', self.rid, 'max', max)

  if max_change > 0 then
    local jids = redis.call('zrevrange', key_pending, 0, max_change - 1, 'withscores')
    local jid_count = #jids
    if jid_count == 0 then
      return self.rid
    end

    for i = 1, jid_count, 2 do
      local new_jid = jids[i]
      local score = jids[i + 1]

      if Qless.job(new_jid):acquire_resources(now) then
        local data = Qless.job(new_jid):data()
        local queue = Qless.queue(data['queue'])
        queue.work.add(score, 0, new_jid)
      end
    end
  end

  return self.rid
end

function QlessResource:unset(now)
  local pending = redis.call('zrevrange', self:prefix('pending'), 0, -1)
  local locks = redis.call('smembers', self:prefix('locks'))

  local deleted = redis.call('del', QlessResource.ns .. self.rid, self:prefix('locks'), self:prefix('pending'))
  redis.call('srem', 'ql:resources', self.rid)

  for _, jid in ipairs(pending) do
    Qless.job(jid):set_failed(now, 'system:fatal', 'Resource ' .. self.rid .. ' no longer exists', 'none', true, true)
  end

  for _, jid in ipairs(locks) do
    local worker = unpack(redis.call('hmget', QlessJob.ns .. jid, 'worker'))
    Qless.job(jid):set_failed(now, 'system:fatal', 'Resource ' .. self.rid .. ' no longer exists', worker, true, true)
  end

  return deleted
end

function QlessResource:prefix(group)
  return QlessResource.ns .. self.rid .. (group and ('-' .. group) or '')
end

function QlessResource:acquire(now, priority, jid)
  local key_locks = self:prefix('locks')
  local max = self:get()
  if max == nil then
    error({ code = 1, msg = 'Acquire(): resource ' .. self.rid .. ' does not exist' })
  end

  redis.call('sadd', 'ql:resources', self.rid)

  if type(jid) ~= 'string' then
    error({ code = 2, msg = "Acquire(): invalid jid; expected string, got '" .. type(jid) .. "'" })
  end

  if redis.call('sismember', self:prefix('locks'), jid) == 1 then
    return true
  end

  local remaining = max - redis.pcall('scard', key_locks)

  if remaining > 0 then
    redis.call('sadd', key_locks, jid)
    redis.call('zrem', self:prefix('pending'), jid)

    return true
  end

  if redis.call('zscore', self:prefix('pending'), jid) == false then
    redis.call('zadd', self:prefix('pending'), priority - (now / 10000000000), jid)
  end

  return false
end

function QlessResource:release(now, jid)
  local key_locks = self:prefix('locks')
  local key_pending = self:prefix('pending')

  redis.call('srem', key_locks, jid)
  redis.call('zrem', key_pending, jid)

  local jids = redis.call('zrevrange', key_pending, 0, 0, 'withscores')
  if #jids == 0 then
    return false
  end

  local new_jid = jids[1]
  local score = jids[2]

  if Qless.job(new_jid):acquire_resources(now) then
    local data = Qless.job(new_jid):data()
    local queue = Qless.queue(data['queue'])
    queue.work.add(score, 0, new_jid)
  end

  return new_jid
end

function QlessResource:locks()
  local locks = redis.call('smembers', self:prefix('locks'))
  table.sort(locks)
  return locks
end

function QlessResource:lock_count()
  return redis.call('scard', self:prefix('locks'))
end

function QlessResource:pending()
  return redis.call('zrevrange', self:prefix('pending'), 0, -1)
end

function QlessResource:pending_count()
  return redis.call('zcard', self:prefix('pending'))
end

function QlessResource:exists()
  return redis.call('exists', self:prefix()) == 1
end

function QlessResource.all_exist(resources)
  for _, res in ipairs(resources) do
    if redis.call('exists', QlessResource.ns .. res) == 0 then
      return false
    end
  end
  return true
end

function QlessResource.pending_counts(now)
  local rids = redis.call('smembers', 'ql:resources')
  table.sort(rids)
  local response = {}
  for _, rid in ipairs(rids) do
    local rname = QlessResource.ns .. rid .. '-pending'
    local count = redis.call('zcard', rname)
    if count > 0 then
      local res_stat = { name = rname, count = count }
      table.insert(response, res_stat)
    end
  end
  return response
end

function QlessResource.locks_counts(now)
  local rids = redis.call('smembers', 'ql:resources')
  table.sort(rids)
  local response = {}
  for _, rid in ipairs(rids) do
    local rname = QlessResource.ns .. rid .. '-locks'
    local count = redis.call('scard', rname)
    if count > 0 then
      local res_stat = { name = rname, count = count }
      table.insert(response, res_stat)
    end
  end
  return response
end
local QlessAPI = {}

local function tonil(value)
  return value ~= '' and value or nil
end

function QlessAPI.get(now, jid)
  local data = Qless.job(jid):data()
  if not data then
    return nil
  end
  return cjson.encode(data)
end

function QlessAPI.multiget(now, ...)
  local results = {}
  for i = 1, select('#', ...) do
    local jid = select(i, ...)
    table.insert(results, Qless.job(jid):data())
  end
  return cjson.encode(results)
end

QlessAPI['config.get'] = function(now, key)
  key = tonil(key)
  if not key then
    return cjson.encode(Qless.config.get(key))
  else
    return Qless.config.get(key)
  end
end

QlessAPI['config.set'] = function(now, key, value)
  key = tonil(key)
  return Qless.config.set(key, value)
end

QlessAPI['config.unset'] = function(now, key)
  key = tonil(key)
  return Qless.config.unset(key)
end

QlessAPI.queues = function(now, queue)
  return cjson.encode(QlessQueue.counts(now, queue))
end

QlessAPI.complete = function(now, jid, worker, queue, data, ...)
  data = tonil(data)
  return Qless.job(jid):complete(now, worker, queue, data, ...)
end

QlessAPI.failed = function(now, group, start, limit)
  group = tonil(group)
  return cjson.encode(Qless.failed(group, start, limit))
end

QlessAPI.fail = function(now, jid, worker, group, message, data)
  data = tonil(data)
  return Qless.job(jid):fail(now, worker, group, message, data)
end

QlessAPI.jobs = function(now, state, ...)
  return Qless.jobs(now, state, ...)
end

QlessAPI.retry = function(now, jid, queue, worker, delay, group, message)
  return Qless.job(jid):retry(now, queue, worker, delay, group, message)
end

QlessAPI.depends = function(now, jid, command, ...)
  return Qless.job(jid):depends(now, command, ...)
end

QlessAPI.heartbeat = function(now, jid, worker, data)
  data = tonil(data)
  return Qless.job(jid):heartbeat(now, worker, data)
end

QlessAPI.workers = function(now, worker)
  return cjson.encode(QlessWorker.counts(now, worker))
end

QlessAPI.track = function(now, command, jid)
  return cjson.encode(Qless.track(now, command, jid))
end

QlessAPI.tag = function(now, command, ...)
  return cjson.encode(Qless.tag(now, command, ...))
end

QlessAPI.stats = function(now, queue, date)
  return cjson.encode(Qless.queue(queue):stats(now, date))
end

QlessAPI.priority = function(now, jid, priority)
  return Qless.job(jid):priority(priority)
end

QlessAPI.log = function(now, jid, message, data)
  assert(jid, "Log(): Argument 'jid' missing")
  assert(message, "Log(): Argument 'message' missing")
  if data then
    data = assert(cjson.decode(data), "Log(): Argument 'data' not cjson: " .. tostring(data))
  end

  local job = Qless.job(jid)
  assert(job:exists(), 'Log(): Job ' .. jid .. ' does not exist')
  job:history(now, message, data)
end

QlessAPI.peek = function(now, queue, count)
  local jids = Qless.queue(queue):peek(now, count)
  local response = {}
  for _, jid in ipairs(jids) do
    table.insert(response, Qless.job(jid):data())
  end
  return cjson.encode(response)
end

QlessAPI.pop = function(now, queue, worker, count)
  local jids = Qless.queue(queue):pop(now, worker, count)
  local response = {}
  for _, jid in ipairs(jids) do
    table.insert(response, Qless.job(jid):data())
  end
  return cjson.encode(response)
end

QlessAPI.pause = function(now, ...)
  return QlessQueue.pause(now, ...)
end

QlessAPI.unpause = function(now, ...)
  return QlessQueue.unpause(...)
end

QlessAPI.paused = function(now, queue)
  return Qless.queue(queue):paused()
end

QlessAPI.cancel = function(now, ...)
  return Qless.cancel(now, ...)
end

QlessAPI.timeout = function(now, ...)
  for i = 1, select('#', ...) do
    local jid = select(i, ...)
    Qless.job(jid):timeout(now)
  end
end

QlessAPI.put = function(now, me, queue, jid, klass, data, delay, ...)
  data = tonil(data)
  return Qless.queue(queue):put(now, me, jid, klass, data, delay, ...)
end

QlessAPI.requeue = function(now, me, queue, jid, ...)
  local job = Qless.job(jid)
  assert(job:exists(), 'Requeue(): Job ' .. jid .. ' does not exist')
  return QlessAPI.put(now, me, queue, jid, ...)
end

QlessAPI.unfail = function(now, queue, group, count)
  return Qless.queue(queue):unfail(now, group, count)
end

QlessAPI.recur = function(now, queue, jid, klass, data, spec, ...)
  data = tonil(data)
  return Qless.queue(queue):recur(now, jid, klass, data, spec, ...)
end

QlessAPI.unrecur = function(now, jid)
  return Qless.recurring(jid):unrecur()
end

QlessAPI['recur.get'] = function(now, jid)
  local data = Qless.recurring(jid):data()
  if not data then
    return nil
  end
  return cjson.encode(data)
end

QlessAPI['recur.update'] = function(now, jid, ...)
  return Qless.recurring(jid):update(now, ...)
end

QlessAPI['recur.tag'] = function(now, jid, ...)
  return Qless.recurring(jid):tag(...)
end

QlessAPI['recur.untag'] = function(now, jid, ...)
  return Qless.recurring(jid):untag(...)
end

QlessAPI.length = function(now, queue)
  return Qless.queue(queue):length()
end

QlessAPI['worker.deregister'] = function(now, ...)
  return QlessWorker.deregister(...)
end

QlessAPI['queue.forget'] = function(now, ...)
  QlessQueue.deregister(...)
end

QlessAPI['resource.set'] = function(now, rid, max)
  return Qless.resource(rid):set(now, max)
end

QlessAPI['resource.get'] = function(now, rid)
  return Qless.resource(rid):get()
end

QlessAPI['resource.data'] = function(now, rid)
  local data = Qless.resource(rid):data()
  if not data then
    return nil
  end

  return cjson.encode(data)
end

QlessAPI['resource.exists'] = function(now, rid)
  return Qless.resource(rid):exists()
end

QlessAPI['resource.unset'] = function(now, rid)
  return Qless.resource(rid):unset(now)
end

QlessAPI['resource.locks'] = function(now, rid)
  local data = Qless.resource(rid):locks()
  if not data then
    return nil
  end

  return cjson.encode(data)
end

QlessAPI['resource.lock_count'] = function(now, rid)
  return Qless.resource(rid):lock_count()
end

QlessAPI['resource.pending'] = function(now, rid)
  local data = Qless.resource(rid):pending()
  if not data then
    return nil
  end

  return cjson.encode(data)
end

QlessAPI['resource.pending_count'] = function(now, rid)
  return Qless.resource(rid):pending_count()
end

QlessAPI['resource.stats_pending'] = function(now)
  return cjson.encode(QlessResource.pending_counts(now))
end

QlessAPI['resource.stats_locks'] = function(now)
  return cjson.encode(QlessResource.locks_counts(now))
end


if #KEYS > 0 then
  error('No Keys should be provided')
end

local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command = assert(QlessAPI[command_name], 'Unknown command ' .. command_name)

local now = tonumber(table.remove(ARGV, 1))
assert(now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

return command(now, unpack(ARGV))
