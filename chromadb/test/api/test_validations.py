import pytest
import numpy as np
from chromadb.api.types import (
    RecordSet,
    record_set_contains_one_of,
    maybe_cast_one_to_many_embedding,
)


def test_does_record_set_contain_any_data() -> None:
    valid_record_set: RecordSet = {
        "ids": ["1", "2", "3"],
        "embeddings": None,
        "metadatas": None,
        "documents": ["doc1", "doc2", "doc3"],
        "images": None,
        "uris": None,
    }

    # Test case 1: Empty-list field
    record_set_non_list: RecordSet = {
        "ids": ["1", "2", "3"],
        "embeddings": [],
        "metadatas": None,
        "documents": None,
        "images": None,
        "uris": None,
    }

    with pytest.raises(ValueError) as e:
        record_set_contains_one_of(record_set_non_list, include=["embeddings"])  # type: ignore[list-item]

    assert "Expected embeddings to be a non-empty list" in str(e)

    # Test case 2: Non-list field
    with pytest.raises(ValueError) as e:
        record_set_contains_one_of(valid_record_set, include=[])

    assert "Expected include to be a non-empty list" in str(e)

    # Test case 3: Non-existent field
    with pytest.raises(ValueError) as e:
        record_set_contains_one_of(valid_record_set, include=["non_existent_field"])  # type: ignore[list-item]

    assert (
        "Expected include key to be a a known field of RecordSet, got non_existent_field"
        in str(e)
    )


def test_maybe_cast_one_to_many_embedding() -> None:
    # Test with None input
    assert maybe_cast_one_to_many_embedding(None) is None

    # Test with a single embedding as a list
    single_embedding = [1.0, 2.0, 3.0]
    result = maybe_cast_one_to_many_embedding(single_embedding)
    assert result == [single_embedding]

    # Test with multiple embeddings as a list of lists
    multiple_embeddings = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
    result = maybe_cast_one_to_many_embedding(multiple_embeddings)  # type: ignore[arg-type]
    assert result == multiple_embeddings

    # Test with a numpy array (single embedding)
    np_single = np.array([1.0, 2.0, 3.0])
    result = maybe_cast_one_to_many_embedding(np_single)
    assert isinstance(result, list)
    assert len(result) == 1
    assert np.array_equal(result[0], np_single)

    # Test with a numpy array (multiple embeddings)
    np_multiple = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    result = maybe_cast_one_to_many_embedding(np_multiple)
    assert isinstance(result, list)
    assert len(result) == 2
    assert np.array_equal(result, np_multiple)

    # Test with an empty list (should raise ValueError)
    with pytest.raises(
        ValueError,
        match="Expected embeddings to be a list or a numpy array with at least one item",
    ):
        maybe_cast_one_to_many_embedding([])

    # Test with an empty list (should raise ValueError)
    with pytest.raises(
        ValueError,
        match="Expected embeddings to be a list or a numpy array with at least one item",
    ):
        maybe_cast_one_to_many_embedding(np.array([]))

    # Test with an empty str (should raise ValueError)
    with pytest.raises(
        ValueError,
        match="Expected embeddings to be a list or a numpy array, got str",
    ):
        maybe_cast_one_to_many_embedding("")  # type: ignore[arg-type]
