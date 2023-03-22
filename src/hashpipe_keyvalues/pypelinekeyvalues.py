from string import Template
import socket
import json
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

    HASH = Template("pypeline://${host}/${inst}/status")
    HASH_re = r"pypeline://(?P<host>[^/]+)/(?P<inst>[^/]+)/status"
    BROADCASTGW = "pypeline:///set"

    def __init__(self, hostname, instance_id, redis_obj):
        self.hostname = hostname
        self.instance_id = instance_id
        self.redis_obj = redis_obj

        self.redis_hash = self.HASH.substitute(host=hostname, inst=instance_id)

    def __str__(self):
        return f"{self.hostname}.{self.instance_id}"

    def __hash__(self):
        return hash(str(self))

    def get(self, keys: list or str = None, fallback=None):
        if isinstance(keys, str):
            val = self.redis_obj.hget(self.redis_hash, keys)
            return HashpipeKeyValues._decode_value(val, fallback=fallback)
        else:
            keyvalues = self.redis_obj.hgetall(self.redis_hash)
            return {
                key: HashpipeKeyValues._decode_value(val, fallback=fallback)
                for key, val in keyvalues.items()
                if keys is None or key in keys
            }

    def set(self, keys: str or list = None, values=None, mapping=None):
        if isinstance(keys, str):
            return self.redis_obj.hset(self.redis_hash, keys, values)
        else:
            if mapping is None:
                mapping = dict(zip(keys, values))
            return self.redis_obj.hset(self.redis_hash, mapping=mapping)

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
        return PypelineKeyValues(*hostname_match_callback(m), redis_obj)

    @staticmethod
    def of(hashpipekv: HashpipeKeyValues):
        return PypelineKeyValues(
            hashpipekv.hostname, hashpipekv.instance_id, hashpipekv.redis_obj
        )

    @staticmethod
    def broadcast(redis_obj, keys: str or list = None, values=None, mapping=None):
        if isinstance(keys, str):
            message = f"{keys}={str(values)}"
        elif mapping is not None:
            message = "\n".join(f"{key}={str(value)}" for key, value in mapping.items())
        else:
            message = "\n".join(f"{key}={str(values[i])}" for i, key in enumerate(keys))
        return redis_obj.publish(PypelineKeyValues.BROADCASTGW, message)

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


def _get_process_states(processes_string_value):
    processes_state_timestamp_dict = json.loads(processes_string_value)
    for process_id, state_timestamp_dict in processes_state_timestamp_dict.items():
        start = state_timestamp_dict["Start"]
        finish = state_timestamp_dict["Finish"]
        error = state_timestamp_dict["Error"]
        if start is None:
            processes_state_timestamp_dict[process_id] = "Idle"
        elif finish is not None and finish > start:
            processes_state_timestamp_dict[process_id] = "Idle"
        elif error is not None and error > start:
            processes_state_timestamp_dict[process_id] = "Error"
        elif finish is not None and finish < start:
            processes_state_timestamp_dict[process_id] = "Busy"
        else:
            processes_state_timestamp_dict[process_id] = "Unknown"

    return processes_state_timestamp_dict


KeyValues_defineKeys(
    PypelineKeyValues,
    {
        "context": (
            "#CONTEXT",
            None,
            None,
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
            lambda self: self.status.startswith("0"),
            False,
            None,
        ),
        "is_alive": (
            "PULSE",
            lambda self: abs(datetime.now() - self.pulse) < timedelta(seconds=1.5),
            False,
            None,
        ),
        "process_states": (
            "PROCESSES",
            lambda self: _get_process_states(self.get("PROCESSES", "{}")),
            False,
            None,
        ),
    },
)
