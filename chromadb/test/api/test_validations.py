import pytest
import numpy as np
from typing import cast
from chromadb.api.types import (
    RecordSet,
    record_set_contains_one_of,
    maybe_cast_one_to_many_embedding,
    validate_embeddings, 
    validate_ids,
    Embeddings,
    IDs,
    errors,
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
    single_embedding = np.array([1.0, 2.0, 3.0])
    result = maybe_cast_one_to_many_embedding(single_embedding)
    assert result == [single_embedding]

    # Test with multiple embeddings as a list of lists
    multiple_embeddings = [np.array([1.0, 2.0, 3.0]), np.array([4.0, 5.0, 6.0])]
    result = maybe_cast_one_to_many_embedding(multiple_embeddings)
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


def test_embeddings_validation() -> None:
    invalid_embeddings = [[0, 0, True], [1.2, 2.24, 3.2]]

    with pytest.raises(ValueError) as e:
        validate_embeddings(invalid_embeddings)  # type: ignore[arg-type]

    assert "Expected each value in the embedding to be a int or float" in str(e)

    invalid_embeddings = [[0, 0, "invalid"], [1.2, 2.24, 3.2]]

    with pytest.raises(ValueError) as e:
        validate_embeddings(invalid_embeddings)  # type: ignore[arg-type]

    assert "Expected each value in the embedding to be a int or float" in str(e)

    with pytest.raises(ValueError) as e:
        validate_embeddings("invalid")  # type: ignore[arg-type]

    assert "Expected embeddings to be a list, got str" in str(e)


def test_0dim_embedding_validation() -> None:
    embds: Embeddings = [[]]  # type: ignore[list-item]
    with pytest.raises(ValueError) as e:
        validate_embeddings(embds)
    assert "Expected each embedding in the embeddings to be a non-empty list" in str(e)


def test_ids_validation() -> None:
    ids = ["id1", "id2", "id3"]
    assert validate_ids(ids) == ids

    with pytest.raises(ValueError, match="Expected IDs to be a list"):
        validate_ids(cast(IDs, "not a list"))

    with pytest.raises(ValueError, match="Expected IDs to be a non-empty list"):
        validate_ids([])

    with pytest.raises(ValueError, match="Expected ID to be a str"):
        validate_ids(cast(IDs, ["id1", 123, "id3"]))

    with pytest.raises(errors.DuplicateIDError, match="Expected IDs to be unique"):
        validate_ids(["id1", "id2", "id1"])

    ids = [
        "id1",
        "id2",
        "id3",
        "id4",
        "id5",
        "id6",
        "id7",
        "id8",
        "id9",
        "id10",
        "id11",
        "id12",
        "id13",
        "id14",
        "id15",
    ] * 2
    with pytest.raises(errors.DuplicateIDError, match="found 15 duplicated IDs: "):
        validate_ids(ids)
