import time
import redis
redis_obj = redis.Redis(host="redishost", decode_responses=True)

import hashpipekeyvalues as hpkv
kv = hpkv.HashpipeKeyValues('cosmic-gpu-0', 1, redis_obj)

tmp = kv.observation_id
test_value = "Ting!!!!"

kv.observation_id = test_value
time.sleep(1)
assert kv.observation_id == test_value

kv.observation_id = tmp
time.sleep(1)
assert kv.observation_id == tmp

print(kv.observation_stempath)