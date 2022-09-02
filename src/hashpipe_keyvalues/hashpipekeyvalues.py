from string import Template
import socket
import re


class HashpipeKeyValues(object):
    """
    This class aims to encapsulate the logic related to accessing 
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


    @staticmethod
    def instance_at(
        ipaddress:str,
        redis_obj,
        hostname_regex: str = r"(?P<hostname>.+)-\d+-(?P<instance_id>.+)",
        dns = None
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
            message = '\n'.join(f"{key}={str(values[i])}" for i, key in enumerate(keys))
        return redis_obj.publish(HashpipeKeyValues.BROADCASTGW, message)

    @staticmethod
    def _decode_value(value):
        if isinstance(value, bytes):
            value = value.decode()
        if len(value) == 0:
            value = None
        try:
            value = float(value)
        except:
            pass
        return value


    def get(self, keys: list or str = None):
        if isinstance(keys, str):
            val = self.redis_obj.hget(self.redis_getchan, keys)
            return HashpipeKeyValues._decode_value(val)
        else:
            keyvalues = self.redis_obj.hgetall(self.redis_getchan)
            return {
                key: HashpipeKeyValues._decode_value(val)
                for key, val in keyvalues.items() if keys is None or key in keys
            }


    def set(self, keys: str or list, values):
        if isinstance(keys, str):
            message = f"{keys}={str(values)}"
        else:
            message = '\n'.join(f"{key}={str(values[i])}" for i, key in enumerate(keys))
        return self.redis_obj.publish(self.redis_setchan, message)

def _add_property(
    class_,
    property_name,
    property_key,
    getter=None,
    setter=None,
    doc=None,
):
    if getter is None:
        assert property_key is not None, f"Cannot use default getter without a key for {property_name}"
        getter = lambda self: self.get(property_key)
    if setter is None:
        assert property_key is not None, f"Cannot use default setter without a key for {property_name}"
        setter = lambda self, value: self.set(property_key, value)
    elif setter is False:
        setter = None

    setattr(
            class_,
            property_name,
            property(
                    fget=getter,
                    fset=setter,
                    fdel=None,
                    doc=doc,
            )
    )

STANDARD_KEYS = {
	"blocksize": ("BLOCSIZE", None, False, None),

    "nof_pols": ("NPOL", None, None, None),
    "nof_bits": ("NBITS", None, None, None),
    "nof_beams": ("NBEAM", None, None, None),
    "nof_antennas": ("NANTS", None, None, None),
    "nof_channels": ("NCHAN", None, None, None),
    
	"observation_nof_channels": ("OBSNCHAN", None, None, None),
	"observation_frequecy": ("OBSFREQ", None, None, None),
	"observation_bandwidth": ("OBSBW", None, None, None),
	"channel_bandwidth": ("CHAN_BW",
        None,
        lambda self, value: self.set(["CHAN_BW", "TBIN"], [value, 1.0/value]),
        None
    ),

	"source": ("SRC_NAME", None, None, None),
	"telescope": ("TELESCOP", None, None, None),
	"data_directory": ("DATADIR", None, False, None),
	"project_id": ("PROJID", None, None, None),
	"backend": ("BACKEND", None, False, None),
	"observation_stem": ("OBSSTEM", None, False, None),
	"observation_stempath": (None,
        lambda self: list(self.get(["DATADIR", "PROJID", "BACKEND", "OBSSTEM"]).keys()),
        False,
        None
    ),

    "channel_timespan": ("TBIN",
        None,
        lambda self, value: self.set(["TBIN", "CHAN_BW"], [value, 1.0/value]),
        None
    ),
	"directio": ("DIRECTIO", None, None, None),
	"packet_index": ("PKTIDX", None, None, None),
	"beam_id": ("BEAM_ID", None, None, None),
	"sample_datatype": ("DATATYPE", None, None, None),
	"rightascension_string": ("RA_STR", None, None, None),
	"declination_string": ("DEC_STR", None, None, None),
	"stt_mjd_day": ("STT_IMJD", None, None, None),
	"stt_mjd_seconds": ("STT_SMJD", None, None, None),

	"observation_id": ("OBSID", None, None, None),
}

for attribute, key in STANDARD_KEYS.items():
    _add_property(HashpipeKeyValues, attribute, *key)
