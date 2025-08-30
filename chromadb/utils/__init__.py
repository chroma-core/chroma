import importlib
from typing import Type, TypeVar, cast

from chromadb.api.types import Document, Documents, Embeddable

C = TypeVar("C")


def get_class(fqn: str, type: Type[C]) -> Type[C]:
    """Given a fully qualifed class name, import the module and return the class"""
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cast(Type[C], cls)


def text_only_embeddable_check(input: Embeddable, embedding_function_name: str) -> Documents:
    """
    Helper function to determine if a given Embeddable is text-only.

    Once the minimum supported python version is bumped up to 3.10, this should
    be replaced with TypeGuard:
    https://docs.python.org/3.10/library/typing.html#typing.TypeGuard
    """
    if not all(isinstance(item, Document) for item in input):
        raise ValueError(f"{embedding_function_name} only supports text documents, not images")
    return cast(Documents, input)
