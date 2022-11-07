import redis
from hashpipe_keyvalues.pypelinekeyvalues import PypelineKeyValues, HashpipeKeyValues

redis_obj = redis.Redis(host="redishost", decode_responses=True)
kv = HashpipeKeyValues.instance_at("192.168.64.100", redis_obj)
pypeline = PypelineKeyValues.of(kv)

print(pypeline.status)
print(pypeline.is_idle)
print(pypeline.stages)
print(pypeline.pulse)
print(pypeline.is_alive)
