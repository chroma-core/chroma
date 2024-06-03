from typing import Generator
import pytest
from chromadb.utils.the_registry import (
    _TheChromaObjectRegistry,
)


@pytest.fixture(autouse=True)
def reset_registry() -> Generator[None, None, None]:
    """
    Reset the registry to nothing before each test, returning it to its original state after.
    This makes sure running tests doesn't do something weird.
    """
    if _TheChromaObjectRegistry._instance is not None:
        registry_state = _TheChromaObjectRegistry._instance.registry
        _TheChromaObjectRegistry._instance.registry = {}

    yield

    if _TheChromaObjectRegistry._instance is not None:
        _TheChromaObjectRegistry._instance.registry = registry_state
