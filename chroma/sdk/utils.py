from types import SimpleNamespace
from typing import Iterable

# enables dot notation for python objects
class nn(SimpleNamespace):
    def __init__(self, dictionary, **kwargs):
        super().__init__(**kwargs)
        for key, value in dictionary.items():
            if isinstance(value, dict):
                self.__setattr__(key, nn(value))
            else:
                self.__setattr__(key, value)

# Convenience function to hoist single elements to lists
def hoist_to_list(item):
    if isinstance(item, str):
        return [item]
    elif isinstance(item, Iterable):
        return item
    else:
        return [item]