from typing import Any, Dict, Optional, Type, TypeVar
from typing_extensions import Self


T = TypeVar("T")


class _TheChromaObjectRegistry:
    _instance: Optional[Self] = None  # This will store the singleton instance
    registry: Dict[str, Any] = {}

    def __new__(cls: Type["_TheChromaObjectRegistry"]) -> "_TheChromaObjectRegistry":
        if cls._instance is None:
            cls._instance = super(_TheChromaObjectRegistry, cls).__new__(cls)
            # Initialize the registry dictionary
            cls._instance.registry = {}
        return cls._instance

    def _register(self, to_register: Type[T]) -> Type[T]:
        object_name = to_register.__name__

        # There can be only one
        if self.registry.get(object_name) is not None:
            raise ValueError(f"Object with name {object_name} already registered.")
        self.registry[to_register.__name__] = to_register
        return to_register

    def _get(self, object_name: str) -> Any:
        return self.registry.get(object_name)


_chroma_object_registry: _TheChromaObjectRegistry = _TheChromaObjectRegistry()


# Decorator for things we want to register
def _register(
    to_register: Type[T],
) -> Type[T]:
    return _chroma_object_registry._register(to_register)


def _get(object_name: str) -> Any:
    return _chroma_object_registry._get(object_name)
