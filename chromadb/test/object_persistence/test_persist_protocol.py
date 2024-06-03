import pytest  # noqa: F401
from typing import Protocol
import json

from chromadb.api.types import StoreAndRegisterProtocolMeta
from chromadb.utils.the_registry import _chroma_object_registry

from fixtures import reset_registry  # noqa: F401


def test_persist_protocol() -> None:
    """
    Test that we can register, recover and create protocol objects
    """

    class ANewProtocol(Protocol, metaclass=StoreAndRegisterProtocolMeta):
        instance_member: int

        def __init__(self, init_pos_arg: int, init_kwarg: int) -> None:
            self.instance_member = init_pos_arg * init_kwarg

        def __call__(self) -> int:
            return self.instance_member

    class ANewConcreteClass(ANewProtocol):
        def __init__(self, init_pos_arg: int, init_kwarg: int) -> None:
            super().__init__(init_pos_arg, init_kwarg)

    # Check that the concrete class is registered
    assert _chroma_object_registry._instance is not None
    assert (
        _chroma_object_registry._instance.registry[ANewConcreteClass.__name__]
        == ANewConcreteClass
    )

    # Create an instance of the protocol
    pos_arg = 3
    kwarg = 4
    instance = ANewConcreteClass(pos_arg, init_kwarg=kwarg)

    # Test that the instance has the args string
    assert instance._init_args == json.dumps(  # type: ignore[attr-defined]
        {"args": [pos_arg], "kwargs": {"init_kwarg": kwarg}}
    )

    # Crate a new instance from the args string
