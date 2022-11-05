from string import Template
import socket
import re
from typing import Callable


class HashpipeKeyValuesCache(object):
    """
    This class aims to encapsulate the logic related to accessing
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
            return {
                key: self.keyvalues.get(key, None)
                for key in keys
            }

    def set(self, keys: str or list, values):
        if isinstance(keys, str):
            keys, values = [keys], [values]

        for i, key in enumerate(keys):
            self.keyvalues[key] = values[i]
        return len(keys)

    @staticmethod
    def _add_property(
        property_name: str,
        key: str,
        valueGetter: Callable,
        valueSetter: Callable,
        valueDocumentation: str,
    ):
        if valueGetter is None:
            assert (
                key is not None
            ), f"Cannot use default getter without a key for {property_name}"
            getter = lambda self: self.get(key)
        else:
            getter = valueGetter

        if valueSetter is None:
            assert (
                key is not None
            ), f"Cannot use default setter without a key for {property_name}"
            setter = lambda self, value: self.set(key, value)
        elif valueSetter is False:
            setter = None
        else:
            setter = valueSetter

        setattr(
            HashpipeKeyValuesCache,
            property_name,
            property(
                fget=getter,
                fset=setter,
                fdel=None,
                doc=valueDocumentation,
            ),
        )


def HashpipeKeyValuesCache_defineKeys(
    keyvalue_propertytuple_dict: "dict[str, tuple]",
):
    for property_name, property_tuple in keyvalue_propertytuple_dict.items():
        HashpipeKeyValuesCache._add_property(property_name, *property_tuple)
