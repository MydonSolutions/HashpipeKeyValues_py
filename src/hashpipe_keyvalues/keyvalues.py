from typing import Callable
import time


class KeyValues(object):
    """
    This class indicates the logic related to accessing
    key-values. Primarily it enables the _add_property method
    to correctly affect classes that inherit from this.
    """

    def __str__(self):
        raise NotImplementedError

    def __hash__(self):
        raise NotImplementedError

    def get(self, keys: list or str = None):
        raise NotImplementedError

    def get_or_error(self, keys: list or str = None, tries=1, retry_period_s=0):
        attempt_no = 0
        while True:
            value = self.get(keys, fallback=None)
            attempt_no += 1
            if value is not None:
                return value
            if attempt_no >= tries:
                raise RuntimeError(
                    f"Could not get value for '{keys}' of {self} ({attempt_no} attempt{'s' if tries > 1 else ''})."
                )
            if retry_period_s > 0:
                time.sleep(retry_period_s)

    def set(self, keys: str or list, values):
        raise NotImplementedError

    @staticmethod
    def _add_property(
        recipient,
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
            recipient,
            property_name,
            property(
                fget=getter,
                fset=setter,
                fdel=None,
                doc=valueDocumentation,
            ),
        )


def KeyValues_defineKeys(
    recipient,  # obj.__class__
    keyvalue_propertytuple_dict: "dict[str, tuple]",
):
    for property_name, property_tuple in keyvalue_propertytuple_dict.items():
        KeyValues._add_property(recipient, property_name, *property_tuple)
