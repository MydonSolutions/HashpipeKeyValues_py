from string import Template
import socket
import re

from keyvaluestore import KeyValueStore
from .hashpipekeyvaluescache import HashpipeKeyValuesCache


class HashpipeKeyValues(KeyValueStore):
    """
    This class encapsulates the logic related to accessing
    standard key-values in Hashpipe"s status-buffer.
    """

    GETGW = Template("hashpipe://${host}/${inst}/status")
    SETGW = Template("hashpipe://${host}/${inst}/set")
    GETGW_re = r"hashpipe://(?P<host>[^/]+)/(?P<inst>[^/]+)/status"
    SETGW_re = r"hashpipe://(?P<host>[^/]+)/(?P<inst>[^/]+)/set"
    BROADCASTGW = "hashpipe:///set"

    def __init__(self, hostname, instance_id, redis_obj):
        self.hostname = hostname
        self.instance_id = instance_id
        self.redis_obj = redis_obj

        self.redis_getchan = self.GETGW.substitute(host=hostname, inst=instance_id)
        self.redis_setchan = self.SETGW.substitute(host=hostname, inst=instance_id)

    def __str__(self):
        return f"{self.hostname}.{self.instance_id}"

    def __hash__(self):
        return hash(str(self))

    def get(self, keys: list or str = None, fallback=None):
        if isinstance(keys, str):
            val = self.redis_obj.hget(self.redis_getchan, keys)
            return HashpipeKeyValues._decode_value(val, fallback=fallback)
        else:
            keyvalues = self.redis_obj.hgetall(self.redis_getchan)
            return {
                key: HashpipeKeyValues._decode_value(val, fallback=fallback)
                for key, val in keyvalues.items()
                if keys is None or key in keys
            }

    def set(self, keys: str or list, values, mapping=None):
        if isinstance(keys, str):
            message = f"{keys}={str(values)}"
        elif mapping is not None:
            message = "\n".join(f"{key}={str(value)}" for key, value in mapping.items())
        else:
            message = "\n".join(f"{key}={str(values[i])}" for i, key in enumerate(keys))
        return self.redis_obj.publish(self.redis_setchan, message), message

    def get_cache(self):
        return HashpipeKeyValuesCache(self.hostname, self.instance_id, self.get())

    @staticmethod
    def from_string(hpt_str: str, redis_obj):
        sepindex = hpt_str.rindex(".")
        return HashpipeKeyValues(hpt_str[:sepindex], hpt_str[sepindex + 1 :], redis_obj)

    @staticmethod
    def instance_at(
        ipaddress: str,
        redis_obj,
        hostname_regex: str = r"(?P<hostname>.+)-\d+g-(?P<instance_id>.+)",
        hostname_match_callback=lambda m: (m.group(1), m.group(2)),
        dns=None,
    ):
        if dns is not None and ipaddress in dns:
            hostname = dns[ipaddress]
        else:
            hostname = socket.gethostbyaddr(ipaddress)[0]

        m = re.match(hostname_regex, hostname)
        assert m is not None, f"'{hostname}' does not match r`{hostname_regex}`"
        return HashpipeKeyValues(*hostname_match_callback(m), redis_obj)

    @staticmethod
    def broadcast(redis_obj, keys: str or list = None, values=None, mapping=None):
        if isinstance(keys, str):
            message = f"{keys}={str(values)}"
        elif mapping is not None:
            message = "\n".join(f"{key}={str(value)}" for key, value in mapping.items())
        else:
            message = "\n".join(f"{key}={str(values[i])}" for i, key in enumerate(keys))
        return redis_obj.publish(HashpipeKeyValues.BROADCASTGW, message)

    @staticmethod
    def _decode_value(value, fallback=None):
        if isinstance(value, bytes):
            value = value.decode()
        if value is None:
            return fallback
        if len(value) == 0:
            return fallback
        try:
            return float(value)
        except:
            return value
