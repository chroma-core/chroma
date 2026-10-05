import pytest

from chromadb.errors import InvalidDimensionException
from chromadb.segment.impl.vector.local_hnsw import LocalHnswSegment


def test_local_hnsw_dimension_mismatch_explains_embedding_space() -> None:
    segment = object.__new__(LocalHnswSegment)
    segment._index = object()
    segment._dimensionality = 3

    with pytest.raises(InvalidDimensionException) as exc_info:
        segment._ensure_index(n=1, dim=4)

    message = str(exc_info.value)
    assert "Dimensionality of (4) does not match index dimensionality (3)" in message
    assert "consistent dimensionality and vector space" in message
    assert "If you changed embedding models, re-embed the collection" in message
