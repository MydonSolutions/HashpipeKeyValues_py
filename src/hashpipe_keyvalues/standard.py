from keyvaluestore import KeyValueStore
from .hashpipekeyvalues import HashpipeKeyValues
from .kvpsets.standard import PROPERTIES as STANDARD_PROPERTIES

KeyValueStore.add_properties(HashpipeKeyValues, STANDARD_PROPERTIES)
