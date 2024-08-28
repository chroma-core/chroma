import pytest
from typing import List, cast
from chromadb.api import ClientAPI
from chromadb.test.api.utils import (
    batch_records,
    minimal_records,
    bad_dimensionality_query,
    operator_records,
    records,
    contains_records,
)
from chromadb.errors import InvalidCollectionException
from chromadb.api.types import IncludeEnum


def test_get_nearest_neighbors(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)  # type: ignore[arg-type]

    includes = cast(
        List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
    )
    nn = collection.query(
        query_embeddings=[1.1, 2.3, 3.2],
        n_results=1,
        where={},
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1  # type: ignore[literal-required]
        elif key == "included":
            assert set(nn[key]) == set(includes)  # type: ignore[literal-required]
        else:
            assert nn[key] is None  # type: ignore[literal-required]

    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],  # type: ignore[arg-type]
        n_results=1,
        where={},
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1  # type: ignore[literal-required]
        elif key == "included":
            assert set(nn[key]) == set(includes)  # type: ignore[literal-required]
        else:
            assert nn[key] is None  # type: ignore[literal-required]

    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2], [0.1, 2.3, 4.5]],  # type: ignore[arg-type]
        n_results=1,
        where={},
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 2  # type: ignore[literal-required]
        elif key == "included":
            assert set(nn[key]) == set(includes)  # type: ignore[literal-required]
        else:
            assert nn[key] is None  # type: ignore[literal-required]


def test_dimensionality_validation_query(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_dimensionality_validation_query")
    collection.add(**minimal_records)  # type: ignore[arg-type]

    with pytest.raises(Exception) as e:
        collection.query(**bad_dimensionality_query)  # type: ignore[arg-type]
    assert "dimensionality" in str(e.value)


def test_query_document_valid_operators(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_valid_operators")
    collection.add(**operator_records)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="where document"):
        collection.query(query_embeddings=[0, 0, 0], where_document={"$contains": 2})  # type: ignore[dict-item]


def test_query_where_document(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_query_where_document")
    collection.add(**contains_records)  # type: ignore[arg-type]

    items = collection.query(
        query_embeddings=[1, 0, 0], where_document={"$contains": "doc1"}, n_results=1
    )
    assert len((items["metadatas"] or [])[0]) == 1

    items = collection.query(
        query_embeddings=[0, 0, 0], where_document={"$contains": "great"}, n_results=2
    )
    assert len((items["metadatas"] or [])[0]) == 2

    with pytest.raises(Exception) as e:
        items = collection.query(
            query_embeddings=[0, 0, 0], where_document={"$contains": "bad"}, n_results=1
        )
        assert "datapoints" in str(e.value)


def test_query_include(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_query_include")
    collection.add(**records)  # type: ignore[arg-type]

    include = cast(List[IncludeEnum], ["metadatas", "documents", "distances"])
    items = collection.query(
        query_embeddings=[0, 0, 0],
        include=include,
        n_results=1,
    )
    assert items["embeddings"] is None
    assert items["ids"][0][0] == "id1"
    assert (items["metadatas"] or [])[0][0]["int_value"] == 1
    assert set(items["included"]) == set(include)

    include = cast(List[IncludeEnum], ["embeddings", "documents", "distances"])
    items = collection.query(
        query_embeddings=[0, 0, 0],
        include=include,
        n_results=1,
    )
    assert items["metadatas"] is None
    assert items["ids"][0][0] == "id1"
    assert set(items["included"]) == set(include)

    items = collection.query(
        query_embeddings=[[0, 0, 0], [1, 2, 1.2]],  # type: ignore[arg-type]
        include=[],
        n_results=2,
    )
    assert items["documents"] is None
    assert items["metadatas"] is None
    assert items["embeddings"] is None
    assert items["distances"] is None
    assert items["ids"][0][0] == "id1"
    assert items["ids"][0][1] == "id2"


# make sure query results are returned in the right order
def test_query_order(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_query_order")
    collection.add(**records)  # type: ignore[arg-type]

    items = collection.query(
        query_embeddings=[1.2, 2.24, 3.2],
        include=cast(List[IncludeEnum], ["metadatas", "documents", "distances"]),
        n_results=2,
    )

    assert (items["documents"] or [])[0][0] == "this document is second"
    assert (items["documents"] or [])[0][1] == "this document is first"


def test_invalid_n_results_param(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**records)  # type: ignore[arg-type]
    with pytest.raises(TypeError) as exc:
        collection.query(
            query_embeddings=[[1.1, 2.3, 3.2]],  # type: ignore[arg-type]
            n_results=-1,
            where={},
            include=cast(
                List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
            ),
        )
    assert "Number of requested results -1, cannot be negative, or zero." in str(
        exc.value
    )
    assert exc.type == TypeError

    with pytest.raises(ValueError) as ve:
        collection.query(
            query_embeddings=[[1.1, 2.3, 3.2]],  # type: ignore[arg-type]
            n_results="one",  # type: ignore[arg-type]
            where={},
            include=cast(
                List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
            ),
        )
    assert "int" in str(ve.value)
    assert ve.type == ValueError


# test to make sure query error on invalid embeddings input
def test_query_invalid_embeddings(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_invalid_embeddings")

    # Query with invalid embeddings
    with pytest.raises(ValueError) as e:
        collection.query(
            query_embeddings=[["1.1", "2.3", "3.2"]],  # type: ignore[arg-type]
            n_results=1,
        )
    assert "embedding" in str(e.value)


def test_get_nearest_neighbors_where_n_results_more_than_element(
    client: ClientAPI,
) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**records)  # type: ignore[arg-type]

    includes: List[IncludeEnum] = cast(
        List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
    )
    results = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],  # type: ignore[arg-type]
        n_results=5,
        where={},
        include=includes,
    )
    for key in results.keys():
        if key in includes or key == "ids":
            assert len(results[key][0]) == 2  # type: ignore[literal-required]
        elif key == "included":
            assert set(results[key]) == set(includes)  # type: ignore[literal-required]
        else:
            assert results[key] is None  # type: ignore[literal-required]


def test_increment_index_on(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)  # type: ignore[arg-type]
    assert collection.count() == 2

    includes: List[IncludeEnum] = cast(
        List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
    )
    # increment index
    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],  # type: ignore[arg-type]
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1  # type: ignore[literal-required]
        elif key == "included":
            assert set(nn[key]) == set(includes)  # type: ignore[literal-required]
        else:
            assert nn[key] is None  # type: ignore[literal-required]


def test_where_validation_query(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_validation")
    with pytest.raises(ValueError, match="where"):
        collection.query(query_embeddings=[0, 0, 0], where={"value": {"nested": "5"}})  # type: ignore[dict-item]


def test_collection_query_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.query(query_texts=["test"])
