import pytest
from chromadb.utils.the_registry import (
    _register,
    _get,
    _TheChromaObjectRegistry,
    _chroma_object_registry,
)

from fixtures import reset_registry  # noqa: F401


def test_singleton() -> None:
    """
    Test that the object registry is a singleton, to be defensive
    """
    registry1 = _TheChromaObjectRegistry()
    registry2 = _TheChromaObjectRegistry()

    assert registry1 is registry2
    assert _chroma_object_registry._instance is registry1
    assert _chroma_object_registry._instance is registry2


def test_register() -> None:
    """
    Test that we can register an object exactly once
    """

    @_register
    class AnObject:
        pass

    assert _get("AnObject") == AnObject

    # Test that we cannot register the same object twice
    with pytest.raises(ValueError):

        @_register
        class AnObject:  # type: ignore[no-redef]
            pass
