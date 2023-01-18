from string import Template
import socket
import re
from datetime import datetime, timedelta

from .keyvalues import KeyValues, KeyValues_defineKeys
from .hashpipekeyvalues import HashpipeKeyValues


class PypelineKeyValues(KeyValues):
    """
    This class encapsulates the logic related to accessing
    standard key-values in the status-buffer of a pypeline.
    See https://github.com/MydonSolutions/hpguppi_pypeline.
    """

    HASH = Template("postprocpype://${host}/${inst}/status")
    HASH_re = r"postprocpype://(?P<host>[^/]+)/(?P<inst>[^/]+)/status"
    BROADCASTGW = "postprocpype:///set"

    def __init__(self, hostname, instance_id, redis_obj):
        self.hostname = hostname
        self.instance_id = instance_id
        self.redis_obj = redis_obj

        self.redis_hash = self.HASH.substitute(host=hostname, inst=instance_id)

    def __str__(self):
        return f"{self.hostname}.{self.instance_id}"

    def __hash__(self):
        return hash(str(self))

    def get(self, keys: list or str = None):
        if isinstance(keys, str):
            val = self.redis_obj.hget(self.redis_hash, keys)
            return PypelineKeyValues._decode_value(val)
        else:
            keyvalues = self.redis_obj.hgetall(self.redis_hash)
            return {
                key: PypelineKeyValues._decode_value(val)
                for key, val in keyvalues.items()
                if keys is None or key in keys
            }

    def set(self, keys: str or list, values):
        if isinstance(keys, str):
            return self.redis_obj.hset(self.redis_hash, keys, values)
        else:
            return self.redis_obj.hset(self.redis_hash, mapping=dict(zip(keys, values)))

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
        return PypelineKeyValues(m.group(1), m.group(2), redis_obj)

    @staticmethod
    def of(hashpipekv: HashpipeKeyValues):
        return PypelineKeyValues(
            hashpipekv.hostname, hashpipekv.instance_id, hashpipekv.redis_obj
        )

    @staticmethod
    def broadcast(redis_obj, keys: str or list, values):
        if isinstance(keys, str):
            message = f"{keys}={str(values)}"
        else:
            message = "\n".join(f"{key}={str(values[i])}" for i, key in enumerate(keys))
        return redis_obj.publish(PypelineKeyValues.BROADCASTGW, message)

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


KeyValues_defineKeys(
    PypelineKeyValues,
    {
        "primary_process": (
            "#PRIMARY",
            None,
            False,
            None,
        ),
        "stages": (
            "#STAGES",
            None,
            lambda self, stages: self.set("#STAGES", " ".join(stages)),
            None,
        ),
        "pulse": (
            "PULSE",
            lambda self: datetime.strptime(
                self.get("PULSE", "1970/01/01 00:00:00"), "%Y/%m/%d %H:%M:%S"
            ),
            False,
            None,
        ),
        "status": (
            "STATUS",
            None,
            False,
            None,
        ),
        "is_idle": (
            "STATUS",
            lambda self: self.status.startswith("WAITING"),
            False,
            None,
        ),
        "is_alive": (
            "PULSE",
            lambda self: abs(datetime.now() - self.pulse) < timedelta(seconds=1.5),
            False,
            None,
        ),
    },
)
