from keyvaluestore import KeyValueStore


class HashpipeKeyValuesCache(KeyValueStore):
    """
    This class encapsulates the logic related to accessing
    standard key-values in a dictionary snapshot of a Hashpipe's status-buffer.
    """

    def __init__(self, hostname, instance_id, statusbuffer_snapshot_dict):
        self.hostname = hostname
        self.instance_id = instance_id
        self.keyvalues = statusbuffer_snapshot_dict

    def __str__(self):
        return f"{self.hostname}.{self.instance_id}:cache"

    def __hash__(self):
        return hash(str(self))

    def get(self, keys: list or str = None):
        if isinstance(keys, str):
            return self.keyvalues.get(keys, None)
        else:
            return {key: self.keyvalues.get(key, None) for key in keys}

    def set(self, keys: str or list, values):
        if isinstance(keys, str):
            keys, values = [keys], [values]

        for i, key in enumerate(keys):
            self.keyvalues[key] = values[i]
        return len(keys)
