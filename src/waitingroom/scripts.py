"""All keys for a queue share a Redis Cluster hash slot."""

CREATE = """
local old = redis.call('GET', KEYS[1])
if old and old ~= ARGV[1] then return -1 end
redis.call('SET', KEYS[1], ARGV[1])
return old and 0 or 1
"""

MUTATE = """
local timeout = redis.call('GET', KEYS[1])
if not timeout then return {'missing'} end
local t = redis.call('TIME')
local now = tonumber(t[1])*1000000 + tonumber(t[2])
local action = ARGV[1]
local uid = ARGV[2]
local function remove(id)
    redis.call('ZREM', KEYS[3], id)
    redis.call('ZREM', KEYS[4], id)
    redis.call('HDEL', KEYS[5], id)
end
if action == 'cleanup' then
    local ids = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, ARGV[3])
    for _, id in ipairs(ids) do remove(id) end
    return {'cleaned', #ids}
end
local raw = redis.call('HGET', KEYS[5], uid)
local member = raw and cjson.decode(raw) or nil
if action == 'leave' then
    remove(uid)
    return {'removed', member and 1 or 0}
end
if member and tonumber(member.deadline_us) <= now then
    remove(uid)
    member = nil
end
if action == 'heartbeat' then
    if not member then return {'absent'} end
    if member.token ~= ARGV[3] then return {'stale'} end
elseif not member then
    local seq = tonumber(redis.call('GET', KEYS[2]) or '0')
    if seq >= 9007199254740991 then return {'exhausted'} end
    seq = redis.call('INCR', KEYS[2])
    member = {user_id=uid, token=ARGV[3], joined_us=string.format('%.0f', now), attributes_json=ARGV[4]}
    redis.call('ZADD', KEYS[3], seq, uid)
end
member.heartbeat_us = string.format('%.0f', now)
member.deadline_us = string.format('%.0f', now + tonumber(timeout))
local encoded = cjson.encode(member)
redis.call('HSET', KEYS[5], uid, encoded)
redis.call('ZADD', KEYS[4], member.deadline_us, uid)
return {'member', encoded}
"""
