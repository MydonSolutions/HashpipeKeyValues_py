import time
import redis

from hashpipe_keyvalues.standard import HashpipeKeyValues
from keyvaluestore import KeyValueStore, KeyValueProperty

redis_obj = redis.Redis(host="redishost", decode_responses=True)

NONSTANDARD_KEYS = [
    KeyValueProperty("bogus", None, lambda self: True, False, None),
]

KeyValueStore.add_properties(HashpipeKeyValues, NONSTANDARD_KEYS)

kv = HashpipeKeyValues("cosmic-gpu-4", 1, redis_obj)
# kv = HashpipeKeyValues.instance_at("192.168.64.100", redis_obj)
print(kv)
# print('kv.observation_stempath:', kv.observation_stempath)
print("kv.observation_id:", kv.observation_id)
print("kv.bogus:", kv.bogus)
assert kv.bogus, "Non standard BOGUS didn't work"

print("kv.channel_bandwidth attr:", getattr(kv, "channel_bandwidth"))

tmp = kv.observation_id
test_value = "Ting!!!!"

kv.observation_id = test_value
time.sleep(1)
assert kv.observation_id == test_value

kv.observation_id = tmp
time.sleep(1)
assert kv.observation_id == tmp

print(kv.observation_stempath)

print(kv.antenna_names)

tmp = kv.channel_bandwidth

for bw in [3.1414, tmp]:
    kv.channel_bandwidth = bw
    time.sleep(1)
    assert kv.channel_bandwidth == 1.0 / kv.channel_timespan
