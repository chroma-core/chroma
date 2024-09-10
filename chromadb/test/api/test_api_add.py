import pytest

from chromadb.api import ClientAPI
from chromadb.test.conftest import reset


def test_add_with_no_ids(client: ClientAPI) -> None:
    reset(client)

    coll = client.create_collection("test")
    coll.add(
        embeddings=[[1, 2, 3], [1, 2, 3], [1, 2, 3]],  # type: ignore
        metadatas=[{"a": 1}, None, {"a": 3}],  # type: ignore
        documents=["a", "b", None],  # type: ignore
    )

    results = coll.get()
    assert len(results["ids"]) == 3

    coll.add(
        embeddings=[[1, 2, 3], [1, 2, 3], [1, 2, 3]],  # type: ignore
        metadatas=[{"a": 1}, None, {"a": 3}],  # type: ignore
        documents=["a", "b", None],  # type: ignore
    )

    results = coll.get()
    assert len(results["ids"]) == 6


def test_add_with_inconsistent_number_of_items(client: ClientAPI) -> None:
    reset(client)

    coll = client.create_collection("test")

    # Test case 1: Inconsistent number of ids
    with pytest.raises(ValueError, match="Inconsistent number of records"):
        coll.add(
            ids=["1", "2"],
            embeddings=[[1, 2, 3], [1, 2, 3], [1, 2, 3]],  # type: ignore
            metadatas=[{"a": 1}, {"a": 2}, {"a": 3}],
            documents=["a", "b", "c"],
        )

    # Test case 2: Inconsistent number of embeddings
    with pytest.raises(ValueError, match="Inconsistent number of records"):
        coll.add(
            ids=["1", "2", "3"],
            embeddings=[[1, 2, 3], [1, 2, 3]],  # type: ignore
            metadatas=[{"a": 1}, {"a": 2}, {"a": 3}],
            documents=["a", "b", "c"],
        )

    # Test case 3: Inconsistent number of metadatas
    with pytest.raises(ValueError, match="Inconsistent number of records"):
        coll.add(
            ids=["1", "2", "3"],
            embeddings=[[1, 2, 3], [1, 2, 3], [1, 2, 3]],  # type: ignore
            metadatas=[{"a": 1}, {"a": 2}],
            documents=["a", "b", "c"],
        )

    # Test case 4: Inconsistent number of documents
    with pytest.raises(ValueError, match="Inconsistent number of records"):
        coll.add(
            ids=["1", "2", "3"],
            embeddings=[[1, 2, 3], [1, 2, 3], [1, 2, 3]],  # type: ignore
            metadatas=[{"a": 1}, {"a": 2}, {"a": 3}],
            documents=["a", "b"],
        )

    # Test case 5: Multiple inconsistencies
    with pytest.raises(ValueError, match="Inconsistent number of records"):
        coll.add(
            ids=["1", "2"],
            embeddings=[[1, 2, 3], [1, 2, 3], [1, 2, 3]],  # type: ignore
            metadatas=[{"a": 1}],
            documents=["a", "b", "c", "d"],
        )


def test_add_with_partial_ids(client: ClientAPI) -> None:
    reset(client)

    coll = client.create_collection("test")

    with pytest.raises(ValueError, match="Expected ID to be a str"):
        coll.add(
            ids=["1", None],  # type: ignore
            embeddings=[[1, 2, 3], [1, 2, 3], [1, 2, 3]],  # type: ignore
            metadatas=[{"a": 1}, None, {"a": 3}],  # type: ignore
            documents=["a", "b", None],  # type: ignore
        )


def test_add_with_no_data(client: ClientAPI) -> None:
    reset(client)

    coll = client.create_collection("test")

    with pytest.raises(
        Exception, match="Expected embeddings to be a list with at least one item"
    ):
        coll.add(
            ids=["1"],
            embeddings=[],
            metadatas=[{"a": 1}],
            documents=[],
        )
