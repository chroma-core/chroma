import pytest

from chromadb.api.segment import SegmentAPI
from chromadb.errors import InvalidDimensionException


def test_dimension_mismatch_explains_embedding_space() -> None:
    api = object.__new__(SegmentAPI)

    with pytest.raises(InvalidDimensionException) as exc_info:
        api._validate_dimension({"dimension": 3}, dim=4, update=False)

    message = str(exc_info.value)
    assert "Embedding dimension 4 does not match collection dimensionality 3" in message
    assert "same vector space" in message
    assert "re-embed all records if you switch models" in message
