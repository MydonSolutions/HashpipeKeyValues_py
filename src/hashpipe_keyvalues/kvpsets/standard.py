from datetime import datetime, timedelta

from keyvaluestore import KeyValueProperty


def _gather_antennaCsvEntries(key_prefix, hpkv, separator: str = ","):
    # manage limited entry length
    nants = hpkv.nof_antennas or 1

    antname_list = []
    key_enum = 0

    while len(antname_list) < nants:
        antnames = hpkv.get(f"{key_prefix}{key_enum:02d}")
        key_enum += 1
        antname_list += antnames.split(separator)

    return antname_list


def _generate_antennaCsvEntries(key_prefix, ant_values, separator: str = ","):
    assert len(key_prefix) <= 6
    # manage limited entry length
    keyvalues = {}
    if len(ant_values) == 0:
        return keyvalues

    key_enum = 0
    current_str = ant_values[0]

    for ant in ant_values[1:]:
        addition = f"{separator}{ant}"
        if len(addition) + len(current_str) > 68:
            keyvalues[f"{key_prefix}%02d" % key_enum] = current_str
            key_enum += 1
            current_str = ant
        else:
            current_str += addition

    keyvalues[f"{key_prefix}%02d" % key_enum] = current_str
    return keyvalues


PROPERTIES = [
    KeyValueProperty("blocksize", "BLOCSIZE", None, False, None),
    KeyValueProperty("nof_pols", "NPOL", None, None, None),
    KeyValueProperty("nof_bits", "NBITS", None, None, None),
    KeyValueProperty("nof_beams", "NBEAM", None, None, None),
    KeyValueProperty("nof_antennas", "NANTS", None, None, None),
    KeyValueProperty("nof_channels", "NCHAN", None, None, None),
    KeyValueProperty("observation_nof_channels", "OBSNCHAN", None, None, None),
    KeyValueProperty("observation_frequency", "OBSFREQ", None, None, None),
    KeyValueProperty("observation_bandwidth", "OBSBW", None, None, None),
    KeyValueProperty(
        "channel_bandwidth",
        "CHAN_BW",
        None,
        lambda self, value: self.set(["CHAN_BW", "TBIN"], [value, 1.0 / value]),
        None,
    ),
    KeyValueProperty("source", "SRC_NAME", None, None, None),
    KeyValueProperty("telescope", "TELESCOP", None, None, None),
    KeyValueProperty("data_directory", "DATADIR", None, False, None),
    KeyValueProperty(
        "project_id",
        "PROJID",
        lambda self: self.get("PROJID", ".")[0:23],
        None,
        None,
    ),
    KeyValueProperty(
        "backend",
        "BACKEND",
        lambda self: self.get("BACKEND", ".")[0:23],
        None,
        None,
    ),
    KeyValueProperty("observation_stem", "OBSSTEM", None, False, None),
    KeyValueProperty(
        "observation_stempath",
        None,
        lambda self: [
            self.data_directory,
            self.project_id,
            self.backend,
            self.observation_stem,
        ],
        False,
        None,
    ),
    KeyValueProperty(
        "channel_timespan",
        "TBIN",
        None,
        lambda self, value: self.set(["TBIN", "CHAN_BW"], [value, 1.0 / value]),
        None,
    ),
    KeyValueProperty("directio", "DIRECTIO", None, None, None),
    KeyValueProperty("packet_index", "PKTIDX", None, None, None),
    KeyValueProperty("beam_id", "BEAM_ID", None, None, None),
    KeyValueProperty("sample_datatype", "DATATYPE", None, None, None),
    KeyValueProperty("rightascension_string", "RA_STR", None, None, None),
    KeyValueProperty("declination_string", "DEC_STR", None, None, None),
    KeyValueProperty("stt_mjd_day", "STT_IMJD", None, None, None),
    KeyValueProperty("stt_mjd_seconds", "STT_SMJD", None, None, None),
    KeyValueProperty("observation_id", "OBSID", None, None, None),
    KeyValueProperty(
        "antenna_names",
        None,
        lambda self: _gather_antennaCsvEntries("ANTNMS", self),
        lambda self, value: self.set(
            *_generate_antennaCsvEntries("ANTNMS", self, value)
        ),
        None,
    ),
    KeyValueProperty(
        "antenna_flags",
        None,
        lambda self: _gather_antennaCsvEntries("ANTFLG", self),
        lambda self, value: self.set(
            *_generate_antennaCsvEntries("ANTFLG", self, value)
        ),
        None,
    ),
    KeyValueProperty(
        "pulse",
        "DAQPULSE",
        lambda self: datetime.strptime(
            self.get("DAQPULSE", "Thu Jan 01 00:00:00 1970"), "%a %b %d %H:%M:%S %Y"
        ),
        False,
        None,
    ),
    KeyValueProperty(
        "is_alive",
        "DAQPULSE",
        lambda self: abs(datetime.now() - self.pulse) < timedelta(seconds=3),
        False,
        None,
    ),
]
