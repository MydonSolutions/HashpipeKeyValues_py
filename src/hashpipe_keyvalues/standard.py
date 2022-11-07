from .keyvalues import KeyValues_defineKeys
from .hashpipekeyvalues import HashpipeKeyValues
from .kvpsets.standard import KEYS as STANDARD_KEYS

KeyValues_defineKeys(HashpipeKeyValues, STANDARD_KEYS)
