import pytest

from chromadb.api.segment import SegmentAPI
from chromadb.errors import InvalidDimensionException


def test_dimension_mismatch_explains_embedding_space() -> None:
    api = object.__new__(SegmentAPI)

    with pytest.raises(InvalidDimensionException) as exc_info:
        api._validate_dimension({"dimension": 3}, dim=4, update=False)

    message = str(exc_info.value)
    assert "Embedding dimension 4 does not match collection dimensionality 3" in message
    assert "consistent dimensionality and vector space" in message
    assert "If you changed embedding models, re-embed the collection" in message
