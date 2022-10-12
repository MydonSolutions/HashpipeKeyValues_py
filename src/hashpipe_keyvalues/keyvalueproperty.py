from dataclasses import dataclass
from typing import Callable


@dataclass
class KeyValueProperty:
    key: str
    valueGetter: Callable
    valueSetter: Callable
    valueDocumentation: str
