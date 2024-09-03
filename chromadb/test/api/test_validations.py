import pytest
from typing import cast
import chromadb.errors as errors
from chromadb.api.types import (
    validate_embeddings,
    Embeddings,
    IDs,
    RecordSet,
    validate_ids,
    validate_record_set_consistency,
)


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
    embds: Embeddings = [[]]
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


def test_validate_record_set_consistency() -> None:
    # Test record set with inconsistent lengths
    inconsistent_record_set: RecordSet = {
        "ids": ["1", "2"],
        "embeddings": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        "metadatas": [{"key": "value1"}, {"key": "value2"}, {"key": "value3"}],
        "documents": ["doc1", "doc2"],
        "images": None,
        "uris": None,
    }
    with pytest.raises(ValueError, match="Inconsistent number of records:"):
        validate_record_set_consistency(inconsistent_record_set)

    # Test record set with empty list
    empty_list_record_set: RecordSet = {
        "ids": ["1", "2", "3"],
        "embeddings": [],
        "metadatas": [{"key": "value1"}, {"key": "value2"}, {"key": "value3"}],
        "documents": ["doc1", "doc2", "doc3"],
        "images": None,
        "uris": None,
    }
    with pytest.raises(
        ValueError, match="Expected field embeddings to be a non-empty list"
    ):
        validate_record_set_consistency(empty_list_record_set)

    # Test record set with non-list value
    non_list_record_set: RecordSet = {
        "ids": ["1", "2", "3"],
        "embeddings": "not a list",  # type: ignore[typeddict-item]
        "metadatas": [{"key": "value1"}, {"key": "value2"}, {"key": "value3"}],
        "documents": ["doc1", "doc2", "doc3"],
        "images": None,
        "uris": None,
    }
    with pytest.raises(ValueError, match="Expected field embeddings to be a list"):
        validate_record_set_consistency(non_list_record_set)

    # Test record set with multiple errors
    multiple_error_record_set: RecordSet = {
        "ids": [],
        "embeddings": "not a list",  # type: ignore[typeddict-item]
        "metadatas": [{"key": "value1"}, {"key": "value2"}],
        "documents": ["doc1"],
        "images": None,
        "uris": None,
    }
    with pytest.raises(ValueError) as exc_info:
        validate_record_set_consistency(multiple_error_record_set)

    assert "Expected field ids to be a non-empty list" in str(exc_info.value)
    assert "Expected field embeddings to be a list" in str(exc_info.value)
    assert "Inconsistent number of records:" in str(exc_info.value)
