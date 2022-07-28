from string import Template

REDISGETGW = Template("hashpipe://${host}/${inst}/status")
REDISGETGW_re = r"hashpipe://(?P<host>[^/]+)/(?P<inst>[^/]+)/status"
REDISSETGW = Template("hashpipe://${host}/${inst}/set")
REDISSETGW_re = r"hashpipe://(?P<host>[^/]+)/(?P<inst>[^/]+)/set"
REDISSET = "hashpipe:///set"

STANDARD_KEYS = {
	"blocksize": "BLOCSIZE",

    "n_pols": "NPOL",
    "n_bits": "NBITS",
    "n_beams": "NBEAMS",
    "n_antennas": "NANTS",
    "n_chans": "NCHAN",
    
	"obs_nchans": "OBSNCHAN",
	"obs_frequecy": "OBSFREQ",
	"obs_bandwidth": "OBSBW",
	"channel_bandwidth": "CHAN_BW",

	"source": "SRC_NAME",
	"telescope": "TELESCOP",
	"data_directory": "DATADIR",
	"project_id": "PROJID",
	"backend": "BACKEND",
	"observation_stem": "OBSSTEM",

	"bintime": "TBIN",
	"directio": "DIRECTIO",
	"packet_index": "PKTIDX",
	"beam_id": "BEAM_ID",
	"sample_datatype": "DATATYPE",
	"rightascension_string": "RA_STR",
	"declination_string": "DEC_STR",
	"stt_mjd_day": "STT_IMJD",
	"stt_mjd_seconds": "STT_SMJD",

	"observation_id": "OBSID",
}

class HashpipeKeyValues(object):
    """
    This class aims to encapsulate the logic related to accessing 
    standard key-values in Hashpipe"s status-buffer.
    """

    def __init__(self, hostname, instance_id, redis_obj):
        self.hostname = hostname
        self.instance_id = instance_id
        self.redis_obj = redis_obj

        self.redis_getchan = REDISGETGW.substitute(host=hostname, inst=instance_id)
        self.redis_setchan = REDISSETGW.substitute(host=hostname, inst=instance_id)


    def _redis_get(self, key):
        # print(f"REDIS: hget({self.redis_getchan}, {key})")
        val = self.redis_obj.hget(self.redis_getchan, key)
        if isinstance(val, bytes):
            val = val.decode()
        if len(val) == 0:
            val = None 
        return val

    def _redis_set(self, key, val):
        # print(f"REDIS: publish({self.redis_setchan}, {key}={str(val)})")
        return self.redis_obj.publish(self.redis_setchan, f"{key}={str(val)}")

def _add_property(
    class_,
    property_name,
    property_key,
    getter=None,
    setter=None,
    doc=None,
):
    if getter is None:
        getter = lambda self: self._redis_get(property_key)
    if setter is None:
        setter = lambda self, value: self._redis_set(property_key, value)

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

for attribute, key in STANDARD_KEYS.items():
    _add_property(HashpipeKeyValues, attribute, key)

_add_property(
    HashpipeKeyValues,
    "observation_stempath",
    None,
    getter = lambda self: [self.data_directory, self.project_id, self.backend, self.observation_stem]
)