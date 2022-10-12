def _gather_antenna_names(hpkv, separator: str = ","):
    # manage limited entry length
    nants = hpkv.nof_antennas or 1

    antname_list = []
    key_enum = 0

    while len(antname_list) < nants:
        antnames = hpkv.get(f"ANTNMS{key_enum:02d}")
        key_enum += 1
        antname_list += antnames.split(separator)

    return antname_list


def _generate_antenna_names(ant_names: list, separator: str = ","):
    # manage limited entry length
    if len(ant_names) == 0:
        return [], []

    antname_dict = {}
    key_enum = 0
    current_str = ant_names[0]

    for ant in ant_names[1:]:
        addition = f"{separator}{ant}"
        if len(addition) + len(current_str) > 68:
            antname_dict[f"ANTNMS{key_enum:02d}"] = current_str
            key_enum += 1
            current_str = ant
        else:
            current_str += addition

    if len(current_str) > 0:
        antname_dict[f"ANTNMS{key_enum:02d}"] = current_str
    return antname_dict.keys(), antname_dict.values()


KEYS = {
    "blocksize": ("BLOCSIZE", None, False, None),
    "nof_pols": ("NPOL", None, None, None),
    "nof_bits": ("NBITS", None, None, None),
    "nof_beams": ("NBEAM", None, None, None),
    "nof_antennas": ("NANTS", None, None, None),
    "nof_channels": ("NCHAN", None, None, None),
    "observation_nof_channels": ("OBSNCHAN", None, None, None),
    "observation_frequecy": ("OBSFREQ", None, None, None),
    "observation_bandwidth": ("OBSBW", None, None, None),
    "channel_bandwidth": (
        "CHAN_BW",
        None,
        lambda self, value: self.set(["CHAN_BW", "TBIN"], [value, 1.0 / value]),
        None,
    ),
    "source": ("SRC_NAME", None, None, None),
    "telescope": ("TELESCOP", None, None, None),
    "data_directory": ("DATADIR", None, False, None),
    "project_id": ("PROJID", lambda self: self.get("PROJID")[0:23], None, None),
    "backend": ("BACKEND", lambda self: self.get("BACKEND")[0:23], None, None),
    "observation_stem": ("OBSSTEM", None, False, None),
    "observation_stempath": (
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
    "channel_timespan": (
        "TBIN",
        None,
        lambda self, value: self.set(["TBIN", "CHAN_BW"], [value, 1.0 / value]),
        None,
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
    "antenna_names": (
        None,
        lambda self: _gather_antenna_names(self),
        lambda self, value: self.set(*_generate_antenna_names(self, value)),
        None,
    ),
}
