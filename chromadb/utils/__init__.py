import importlib
from typing import Callable, Generic, Optional, Type, TypeVar, cast

C = TypeVar("C")
T = TypeVar("T")


def get_class(fqn: str, type: Type[C]) -> Type[C]:
    """Given a fully qualifed class name, import the module and return the class"""
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cast(Type[C], cls)


class LazyClassAttribute(Generic[T]):
    """A class attribute computed on first access instead of at import time.

    Some class attributes cannot be computed when the module is imported:
    Path.home() raises when $HOME is unset and the uid has no passwd
    entry, which breaks `import chromadb` in environments (systemd
    DynamicUser units, distroless containers) that never touch the
    attribute. Deferring the computation to first use keeps the import
    working and fails only if the attribute is actually needed.
    """

    def __init__(self, factory: Callable[[], T]) -> None:
        self._factory = factory
        self._value: Optional[T] = None
        self._computed = False

    def __get__(self, instance: object, owner: Optional[type] = None) -> T:
        if not self._computed:
            self._value = self._factory()
            self._computed = True
        return cast(T, self._value)
