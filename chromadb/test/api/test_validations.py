import pytest
from typing import cast
import chromadb.errors as errors
from chromadb.api.types import validate_embeddings, Embeddings, IDs, validate_ids


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
