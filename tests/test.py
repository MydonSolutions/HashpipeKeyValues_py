import time
import redis
redis_obj = redis.Redis(host="redishost", decode_responses=True)

from hashpipe_keyvalues import HashpipeKeyValues
# kv = HashpipeKeyValues('cosmic-gpu-0', 1, redis_obj)
kv = HashpipeKeyValues.instance_at("192.168.64.100", redis_obj)
print(kv)

tmp = kv.observation_id
test_value = "Ting!!!!"

kv.observation_id = test_value
time.sleep(1)
assert kv.observation_id == test_value

kv.observation_id = tmp
time.sleep(1)
assert kv.observation_id == tmp

print(kv.observation_stempath)

tmp = kv.channel_bandwidth

for bw in [3.1414, tmp]:
	kv.channel_bandwidth = bw
	time.sleep(1)
	assert kv.channel_bandwidth == 1.0/kv.channel_timespan
