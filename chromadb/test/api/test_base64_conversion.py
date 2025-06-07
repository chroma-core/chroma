import pybase64
import struct
from typing import List, Union
from chromadb.api.types import embeddings_to_base64_bytes


def test_single_embedding_encoding() -> None:
    """Test encoding a single embedding to base64."""
    embeddings: List[List[float]] = [[1.0, 2.0, 3.0, 4.0]]

    result = embeddings_to_base64_bytes(embeddings)  # type: ignore

    assert len(result) == 1
    assert isinstance(result[0], str)

    decoded_bytes = pybase64.b64decode(result[0])
    decoded_floats = struct.unpack("<4f", decoded_bytes)

    for original, decoded in zip(embeddings[0], decoded_floats):
        assert abs(original - decoded) < 1e-6


def test_multiple_embeddings_encoding() -> None:
    """Test encoding multiple embeddings to base64."""
    embeddings: List[List[float]] = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]

    result = embeddings_to_base64_bytes(embeddings)  # type: ignore

    assert len(result) == 2
    assert all(isinstance(encoded, str) for encoded in result)


def test_embedding_with_none() -> None:
    """Test encoding when some embeddings are None."""
    embeddings: List[Union[List[float], None]] = [
        [1.0, 2.0, 3.0],
        None,
        [4.0, 5.0, 6.0],
    ]

    result = embeddings_to_base64_bytes(embeddings)  # type: ignore

    assert len(result) == 3
    assert isinstance(result[0], str)
    assert result[1] is None
    assert isinstance(result[2], str)


def test_empty_embeddings_list() -> None:
    """Test encoding an empty list of embeddings."""
    embeddings: List[List[float]] = []

    result = embeddings_to_base64_bytes(embeddings)  # type: ignore

    assert result == []
