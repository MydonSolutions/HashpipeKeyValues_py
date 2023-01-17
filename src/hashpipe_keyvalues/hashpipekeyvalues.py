from string import Template
import socket
import re

from .keyvalues import KeyValues
from .hashpipekeyvaluescache import HashpipeKeyValuesCache


class HashpipeKeyValues(KeyValues):
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

    def get(self, keys: list or str = None):
        if isinstance(keys, str):
            val = self.redis_obj.hget(self.redis_getchan, keys)
            return HashpipeKeyValues._decode_value(val)
        else:
            keyvalues = self.redis_obj.hgetall(self.redis_getchan)
            return {
                key: HashpipeKeyValues._decode_value(val)
                for key, val in keyvalues.items()
                if keys is None or key in keys
            }

    def set(self, keys: str or list, values):
        if isinstance(keys, str):
            message = f"{keys}={str(values)}"
        else:
            message = "\n".join(f"{key}={str(values[i])}" for i, key in enumerate(keys))
        return self.redis_obj.publish(self.redis_setchan, message), message

    def get_cache(self):
        return HashpipeKeyValuesCache(self.hostname, self.instance_id, self.get())

    @staticmethod
    def from_string(hpt_str: str, redis_obj):
        sepindex = hpt_str.rindex(".")
        return HashpipeKeyvalues(hpt_str[:sepindex], hpt_str[sepindex + 1 :], redis_obj)

    @staticmethod
    def instance_at(
        ipaddress: str,
        redis_obj,
        hostname_regex: str = r"(?P<hostname>.+)-\d+g-(?P<instance_id>.+)",
        dns=None,
    ):
        if dns is not None and ipaddress in dns:
            hostname = dns[ipaddress]
        else:
            hostname = socket.gethostbyaddr(ipaddress)[0]

        m = re.match(hostname_regex, hostname)
        assert m is not None, f"'{hostname}' does not match r`{hostname_regex}`"
        return HashpipeKeyValues(m.group(1), m.group(2), redis_obj)

    @staticmethod
    def broadcast(redis_obj, keys: str or list, values):
        if isinstance(keys, str):
            message = f"{keys}={str(values)}"
        else:
            message = "\n".join(f"{key}={str(values[i])}" for i, key in enumerate(keys))
        return redis_obj.publish(HashpipeKeyValues.BROADCASTGW, message)

    @staticmethod
    def _decode_value(value):
        if isinstance(value, bytes):
            value = value.decode()
        if value is None:
            return value
        if len(value) == 0:
            return None
        try:
            return float(value)
        except:
            return value
