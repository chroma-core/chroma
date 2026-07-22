# type: ignore
import pytest

from chromadb.api.types import QueryResult

metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
}

bad_metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [{"value": {"nested": "5"}}, {"value": [1, 2, 3]}],
}

operator_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}


def test_metadata_cru(client):
    client.reset()
    metadata_a = {"a": 1, "b": 2}
    collection = client.create_collection("testspace", metadata=metadata_a)
    assert collection.metadata is not None
    assert collection.metadata["a"] == 1
    assert collection.metadata["b"] == 2
    collection = client.get_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 1
    assert collection.metadata["b"] == 2
    collection.modify(metadata={"a": 2, "c": 3})
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3
    assert "b" not in collection.metadata
    collection = client.get_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3
    assert "b" not in collection.metadata
    collection = client.get_or_create_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3
    collection = client.get_or_create_collection("testspace2")
    assert collection.metadata is None
    collections = client.list_collections()
    for collection in collections:
        if collection.name == "testspace":
            assert collection.metadata is not None
            assert collection.metadata["a"] == 2
            assert collection.metadata["c"] == 3
        elif collection.name == "testspace2":
            assert collection.metadata is None


def test_metadata_add_get_int_float(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)
    items = collection.get(ids=["id1", "id2"])
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["float_value"] == 1.001
    assert items["metadatas"][1]["int_value"] == 2
    assert isinstance(items["metadatas"][0]["int_value"], int)
    assert isinstance(items["metadatas"][0]["float_value"], float)


def test_metadata_add_query_int_float(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)
    items: QueryResult = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]], n_results=1
    )
    assert items["metadatas"] is not None
    assert items["metadatas"][0][0]["int_value"] == 1
    assert items["metadatas"][0][0]["float_value"] == 1.001
    assert isinstance(items["metadatas"][0][0]["int_value"], int)
    assert isinstance(items["metadatas"][0][0]["float_value"], float)


def test_metadata_get_where_string(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)
    items = collection.get(where={"string_value": "one"})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"


def test_metadata_get_where_int(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)
    items = collection.get(where={"int_value": 1})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"


def test_metadata_get_where_float(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)
    items = collection.get(where={"float_value": 1.001})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"
    assert items["metadatas"][0]["float_value"] == 1.001


def test_metadata_validation_add(client):
    client.reset()
    collection = client.create_collection("test_metadata_validation")
    with pytest.raises(ValueError, match="metadata"):
        collection.add(**bad_metadata_records)


def test_metadata_validation_update(client):
    client.reset()
    collection = client.create_collection("test_metadata_validation")
    collection.add(**metadata_records)
    with pytest.raises(ValueError, match="metadata"):
        collection.update(ids=["id1"], metadatas={"value": {"nested": "5"}})


def test_list_metadata_validation():
    from chromadb.api.types import validate_metadata, validate_update_metadata

    validate_metadata({"tags": ["action", "comedy"]})
    validate_metadata({"scores": [1, 2, 3]})
    validate_metadata({"ratings": [4.5, 3.2]})
    validate_metadata({"flags": [True, False, True]})
    validate_metadata({"tags": ["a", "b"], "count": 5, "name": "test"})
    with pytest.raises(ValueError, match="non-empty"):
        validate_metadata({"tags": []})
    with pytest.raises(ValueError, match="same type"):
        validate_metadata({"tags": ["a", 1]})
    with pytest.raises(ValueError, match="same type"):
        validate_metadata({"vals": [1, 1.5]})
    with pytest.raises(ValueError, match="same type"):
        validate_metadata({"vals": [True, "yes"]})
    with pytest.raises(ValueError, match="str, int, float, or bool"):
        validate_metadata({"tags": [["nested"]]})
    validate_update_metadata({"tags": ["action", "comedy"]})
    validate_update_metadata({"scores": [1, 2, 3]})
    validate_update_metadata({"tags": None})
    with pytest.raises(ValueError, match="non-empty"):
        validate_update_metadata({"tags": []})


def test_array_metadata_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_array_metadata_e2e")
    collection.add(
        ids=["id1", "id2", "id3"],
        embeddings=[[1, 0, 0], [0, 1, 0], [0, 0, 1]],
        metadatas=[
            {"tags": ["action", "comedy"], "year": 2020},
            {"tags": ["drama"], "year": 2021},
            {"tags": ["action", "thriller"], "year": 2022},
        ],
    )
    items = collection.get(ids=["id1"])
    assert len(items["metadatas"]) == 1
    assert sorted(items["metadatas"][0]["tags"]) == ["action", "comedy"]
    assert items["metadatas"][0]["year"] == 2020
    items = collection.get(where={"tags": {"$contains": "action"}})
    ids = sorted([m["year"] for m in items["metadatas"]])
    assert ids == [2020, 2022]
    items = collection.get(where={"tags": {"$contains": "drama"}})
    assert len(items["metadatas"]) == 1
    assert items["metadatas"][0]["year"] == 2021
    items = collection.get(where={"tags": {"$not_contains": "action"}})
    assert len(items["metadatas"]) == 1
    assert items["metadatas"][0]["year"] == 2021
    items = collection.get(where={"tags": {"$contains": "romance"}})
    assert len(items["metadatas"]) == 0


def test_array_metadata_int_contains_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_int_array_e2e")
    collection.add(
        ids=["id1", "id2"],
        embeddings=[[1, 0, 0], [0, 1, 0]],
        metadatas=[
            {"scores": [10, 20, 30]},
            {"scores": [40, 50]},
        ],
    )
    items = collection.get(where={"scores": {"$contains": 20}})
    assert len(items["ids"]) == 1
    assert items["ids"][0] == "id1"
    items = collection.get(where={"scores": {"$contains": 50}})
    assert len(items["ids"]) == 1
    assert items["ids"][0] == "id2"
    items = collection.get(where={"scores": {"$not_contains": 10}})
    assert len(items["ids"]) == 1
    assert items["ids"][0] == "id2"


def test_array_metadata_update_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_array_update_e2e")
    collection.add(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"tags": ["old_a", "old_b"]}],
    )
    items = collection.get(where={"tags": {"$contains": "old_a"}})
    assert len(items["ids"]) == 1
    collection.update(
        ids=["id1"],
        metadatas=[{"tags": ["new_x"]}],
    )
    items = collection.get(where={"tags": {"$contains": "old_a"}})
    assert len(items["ids"]) == 0
    items = collection.get(where={"tags": {"$contains": "new_x"}})
    assert len(items["ids"]) == 1
    items = collection.get(ids=["id1"])
    assert items["metadatas"][0]["tags"] == ["new_x"]


def test_array_metadata_mixed_with_scalar_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_mixed_e2e")
    collection.add(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"name": "Alice", "score": 42, "tags": ["admin", "user"]}],
    )
    items = collection.get(ids=["id1"])
    md = items["metadatas"][0]
    assert md["name"] == "Alice"
    assert md["score"] == 42
    assert sorted(md["tags"]) == ["admin", "user"]
    items = collection.get(where={"score": {"$eq": 42}})
    assert len(items["ids"]) == 1
    items = collection.get(where={"tags": {"$contains": "admin"}})
    assert len(items["ids"]) == 1
    items = collection.get(
        where={
            "$and": [
                {"score": {"$gte": 40}},
                {"tags": {"$contains": "admin"}},
            ]
        }
    )
    assert len(items["ids"]) == 1


def test_metadata_type_change_scalar_to_array_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_scalar_to_array")
    collection.add(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"tags": "old_scalar"}],
    )
    collection.update(
        ids=["id1"],
        metadatas=[{"tags": ["new_a", "new_b"]}],
    )
    items = collection.get(where={"tags": {"$eq": "old_scalar"}})
    assert len(items["ids"]) == 0, "Stale scalar value should have been removed"
    items = collection.get(where={"tags": {"$contains": "new_a"}})
    assert len(items["ids"]) == 1
    items = collection.get(ids=["id1"])
    assert sorted(items["metadatas"][0]["tags"]) == ["new_a", "new_b"]


def test_metadata_type_change_array_to_scalar_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_array_to_scalar")
    collection.add(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"tags": ["old_a", "old_b"]}],
    )
    collection.update(
        ids=["id1"],
        metadatas=[{"tags": "new_scalar"}],
    )
    items = collection.get(where={"tags": {"$contains": "old_a"}})
    assert len(items["ids"]) == 0, "Stale array rows should have been removed"
    items = collection.get(where={"tags": {"$eq": "new_scalar"}})
    assert len(items["ids"]) == 1
    items = collection.get(ids=["id1"])
    assert items["metadatas"][0]["tags"] == "new_scalar"


def test_metadata_type_change_via_upsert_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_upsert_type_change")
    collection.upsert(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"tags": "scalar_val"}],
    )
    collection.upsert(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"tags": ["arr_a", "arr_b"]}],
    )
    items = collection.get(where={"tags": {"$eq": "scalar_val"}})
    assert len(items["ids"]) == 0, "Stale scalar from upsert should be removed"
    items = collection.get(where={"tags": {"$contains": "arr_a"}})
    assert len(items["ids"]) == 1
    items = collection.get(ids=["id1"])
    assert sorted(items["metadatas"][0]["tags"]) == ["arr_a", "arr_b"]


def test_metadata_rapid_type_flip_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_rapid_flip")
    collection.add(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"tags": "original"}],
    )
    collection.update(
        ids=["id1"],
        metadatas=[{"tags": ["mid_a"]}],
    )
    collection.update(
        ids=["id1"],
        metadatas=[{"tags": "final_scalar"}],
    )
    items = collection.get(where={"tags": {"$eq": "original"}})
    assert len(items["ids"]) == 0, "Original scalar should be gone"
    items = collection.get(where={"tags": {"$contains": "mid_a"}})
    assert len(items["ids"]) == 0, "Intermediate array should be gone"
    items = collection.get(where={"tags": {"$eq": "final_scalar"}})
    assert len(items["ids"]) == 1
    items = collection.get(ids=["id1"])
    assert items["metadatas"][0]["tags"] == "final_scalar"


def test_metadata_mixed_simultaneous_type_changes_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_mixed_type_change")
    collection.add(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"color": "red", "tags": ["old_a", "old_b"]}],
    )
    collection.update(
        ids=["id1"],
        metadatas=[{"color": ["blue", "green"], "tags": "new_scalar"}],
    )
    items = collection.get(where={"color": {"$eq": "red"}})
    assert len(items["ids"]) == 0, "Old color scalar should be gone"
    items = collection.get(where={"color": {"$contains": "blue"}})
    assert len(items["ids"]) == 1
    items = collection.get(where={"tags": {"$contains": "old_a"}})
    assert len(items["ids"]) == 0, "Old tags array should be gone"
    items = collection.get(where={"tags": {"$eq": "new_scalar"}})
    assert len(items["ids"]) == 1


def test_metadata_delete_after_type_change_e2e(client):
    if _is_python_local_segment(client):
        pytest.skip("Python local segment does not support array metadata yet")
    client.reset()
    collection = client.create_collection("test_delete_after_type_change")
    collection.add(
        ids=["id1"],
        embeddings=[[1, 0, 0]],
        metadatas=[{"tags": "scalar_val", "keep": "yes"}],
    )
    collection.update(
        ids=["id1"],
        metadatas=[{"tags": ["arr_a", "arr_b"]}],
    )
    collection.update(
        ids=["id1"],
        metadatas=[{"tags": None}],
    )
    items = collection.get(ids=["id1"])
    assert len(items["ids"]) == 1
    assert (
        "tags" not in items["metadatas"][0]
    ), f"tags key should be deleted, got {items['metadatas'][0]}"
    assert items["metadatas"][0]["keep"] == "yes"
    items = collection.get(where={"tags": {"$eq": "scalar_val"}})
    assert len(items["ids"]) == 0
    items = collection.get(where={"tags": {"$contains": "arr_a"}})
    assert len(items["ids"]) == 0


def test_search_result_rows() -> None:
    from chromadb.api.types import SearchResult

    result = SearchResult(
        {
            "ids": [["id1", "id2", "id3"]],
            "documents": [["doc1", "doc2", "doc3"]],
            "embeddings": [[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]],
            "metadatas": [[{"key": "a"}, {"key": "b"}, {"key": "c"}]],
            "scores": [[0.9, 0.8, 0.7]],
            "select": [["document", "score", "metadata"]],
        }
    )
    rows = result.rows()
    assert len(rows) == 1
    assert len(rows[0]) == 3
    assert rows[0][0]["id"] == "id1"
    assert rows[0][0]["document"] == "doc1"
    assert rows[0][0]["embedding"] == [1.0, 2.0]
    assert rows[0][0]["metadata"] == {"key": "a"}
    assert rows[0][0]["score"] == 0.9
    for row in rows[0]:
        assert "id" in row
        assert "document" in row
        assert "embedding" in row
        assert "metadata" in row
        assert "score" in row
    result = SearchResult(
        {
            "ids": [["a1", "a2"], ["b1", "b2", "b3"]],
            "documents": [["doc_a1", "doc_a2"], ["doc_b1", "doc_b2", "doc_b3"]],
            "embeddings": [None, [[1.0], [2.0], [3.0]]],
            "metadatas": [[{"x": 1}, {"x": 2}], None],
            "scores": [[0.5, 0.4], [0.9, 0.8, 0.7]],
            "select": [["document", "score"], ["embedding", "score"]],
        }
    )
    rows = result.rows()
    assert len(rows) == 2
    assert len(rows[0]) == 2
    assert len(rows[1]) == 3
    assert rows[0][0] == {
        "id": "a1",
        "document": "doc_a1",
        "metadata": {"x": 1},
        "score": 0.5,
    }
    assert rows[0][1] == {
        "id": "a2",
        "document": "doc_a2",
        "metadata": {"x": 2},
        "score": 0.4,
    }
    assert rows[1][0] == {
        "id": "b1",
        "document": "doc_b1",
        "embedding": [1.0],
        "score": 0.9,
    }
    assert rows[1][1] == {
        "id": "b2",
        "document": "doc_b2",
        "embedding": [2.0],
        "score": 0.8,
    }
    assert rows[1][2] == {
        "id": "b3",
        "document": "doc_b3",
        "embedding": [3.0],
        "score": 0.7,
    }
    result = SearchResult(
        {
            "ids": [],
            "documents": [],
            "embeddings": [],
            "metadatas": [],
            "scores": [],
            "select": [],
        }
    )
    rows = result.rows()
    assert rows == []
    result = SearchResult(
        {
            "ids": [["id1", "id2", "id3"]],
            "documents": [[None, "doc2", None]],
            "embeddings": None,
            "metadatas": [[{"a": 1}, None, {"c": 3}]],
            "scores": [[0.9, None, 0.7]],
            "select": [["document", "metadata", "score"]],
        }
    )
    rows = result.rows()
    assert len(rows) == 1
    assert len(rows[0]) == 3
    assert rows[0][0] == {"id": "id1", "metadata": {"a": 1}, "score": 0.9}
    assert rows[0][1] == {"id": "id2", "document": "doc2"}
    assert rows[0][2] == {"id": "id3", "metadata": {"c": 3}, "score": 0.7}
    result = SearchResult(
        {
            "ids": [["id1", "id2"]],
            "documents": None,
            "embeddings": None,
            "metadatas": None,
            "scores": None,
            "select": [[]],
        }
    )
    rows = result.rows()
    assert len(rows) == 1
    assert len(rows[0]) == 2
    assert rows[0][0] == {"id": "id1"}
    assert rows[0][1] == {"id": "id2"}
    result = SearchResult(
        {
            "ids": [["test"]],
            "documents": [["test doc"]],
            "metadatas": [[{"test": True}]],
            "embeddings": [[[0.1, 0.2]]],
            "scores": [[0.99]],
            "select": [["all"]],
        }
    )
    assert result["ids"] == [["test"]]
    assert result.get("documents") == [["test doc"]]
    assert "metadatas" in result
    assert len(result) == 6
    rows = result.rows()
    assert len(rows[0]) == 1
    assert rows[0][0]["id"] == "test"


def test_rrf_to_dict() -> None:
    import pytest
    from chromadb.execution.expression.operator import Rrf, Knn, Val

    rrf = Rrf(
        [
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], key="sparse_embedding", return_rank=True),
        ]
    )
    result = rrf.to_dict()
    expected = {
        "$mul": [
            {"$val": -1},
            {
                "$sum": [
                    {
                        "$div": {
                            "left": {"$val": 1.0},
                            "right": {
                                "$sum": [
                                    {"$val": 60},
                                    {
                                        "$knn": {
                                            "query": [0.1, 0.2],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                    {
                        "$div": {
                            "left": {"$val": 1.0},
                            "right": {
                                "$sum": [
                                    {"$val": 60},
                                    {
                                        "$knn": {
                                            "query": [0.3, 0.4],
                                            "key": "sparse_embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                ]
            },
        ]
    }
    assert result == expected
    rrf_weighted = Rrf(
        ranks=[
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], key="sparse_embedding", return_rank=True),
        ],
        weights=[2.0, 1.0],
        k=100,
    )
    result_weighted = rrf_weighted.to_dict()
    expected_weighted = {
        "$mul": [
            {"$val": -1},
            {
                "$sum": [
                    {
                        "$div": {
                            "left": {"$val": 2.0},
                            "right": {
                                "$sum": [
                                    {"$val": 100},
                                    {
                                        "$knn": {
                                            "query": [0.1, 0.2],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                    {
                        "$div": {
                            "left": {"$val": 1.0},
                            "right": {
                                "$sum": [
                                    {"$val": 100},
                                    {
                                        "$knn": {
                                            "query": [0.3, 0.4],
                                            "key": "sparse_embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                ]
            },
        ]
    }
    assert result_weighted == expected_weighted
    rrf_three = Rrf(
        [
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], key="sparse_embedding", return_rank=True),
            Val(5.0),
        ]
    )
    result_three = rrf_three.to_dict()
    assert "$mul" in result_three
    assert "$sum" in result_three["$mul"][1]
    terms = result_three["$mul"][1]["$sum"]
    assert len(terms) == 3
    with pytest.raises(
        ValueError, match="Number of weights .* must match number of ranks"
    ):
        rrf_bad = Rrf(
            ranks=[
                Knn(query=[0.1, 0.2], return_rank=True),
                Knn(query=[0.3, 0.4], return_rank=True),
            ],
            weights=[1.0],
        )
        rrf_bad.to_dict()
    with pytest.raises(ValueError, match="All weights must be non-negative"):
        rrf_negative = Rrf(
            ranks=[
                Knn(query=[0.1, 0.2], return_rank=True),
                Knn(query=[0.3, 0.4], return_rank=True),
            ],
            weights=[1.0, -1.0],
        )
        rrf_negative.to_dict()
    with pytest.raises(ValueError, match="RRF requires at least one rank"):
        rrf_empty = Rrf([])
        rrf_empty.to_dict()
    with pytest.raises(ValueError, match="k must be positive"):
        rrf_neg_k = Rrf([Val(1.0)], k=-5)
        rrf_neg_k.to_dict()
    with pytest.raises(ValueError, match="k must be positive"):
        rrf_zero_k = Rrf([Val(1.0)], k=0)
        rrf_zero_k.to_dict()
    rrf_normalized = Rrf(
        ranks=[
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], key="sparse_embedding", return_rank=True),
        ],
        weights=[3.0, 1.0],
        normalize=True,
        k=100,
    )
    result_normalized = rrf_normalized.to_dict()
    expected_normalized = {
        "$mul": [
            {"$val": -1},
            {
                "$sum": [
                    {
                        "$div": {
                            "left": {"$val": 0.75},
                            "right": {
                                "$sum": [
                                    {"$val": 100},
                                    {
                                        "$knn": {
                                            "query": [0.1, 0.2],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                    {
                        "$div": {
                            "left": {"$val": 0.25},
                            "right": {
                                "$sum": [
                                    {"$val": 100},
                                    {
                                        "$knn": {
                                            "query": [0.3, 0.4],
                                            "key": "sparse_embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                ]
            },
        ]
    }
    assert result_normalized == expected_normalized
    rrf_normalize_defaults = Rrf(
        ranks=[
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], return_rank=True),
        ],
        normalize=True,
    )
    result_defaults = rrf_normalize_defaults.to_dict()
    expected_defaults = {
        "$mul": [
            {"$val": -1},
            {
                "$sum": [
                    {
                        "$div": {
                            "left": {"$val": 0.5},
                            "right": {
                                "$sum": [
                                    {"$val": 60},
                                    {
                                        "$knn": {
                                            "query": [0.1, 0.2],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                    {
                        "$div": {
                            "left": {"$val": 0.5},
                            "right": {
                                "$sum": [
                                    {"$val": 60},
                                    {
                                        "$knn": {
                                            "query": [0.3, 0.4],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                ]
            },
        ]
    }
    assert result_defaults == expected_defaults
    with pytest.raises(ValueError, match="Sum of weights must be positive"):
        rrf_zero_weights = Rrf(
            ranks=[
                Knn(query=[0.1, 0.2], return_rank=True),
                Knn(query=[0.3, 0.4], return_rank=True),
            ],
            weights=[0.0, 0.0],
            normalize=True,
        )
        rrf_zero_weights.to_dict()


def test_validate_sparse_vector():
    from chromadb.base_types import SparseVector

    SparseVector(indices=[0, 2, 5], values=[0.1, 0.5, 0.9])
    SparseVector(indices=[], values=[])
    with pytest.raises(ValueError, match="Expected SparseVector indices to be a list"):
        SparseVector(indices="not_a_list", values=[0.1, 0.2])
    with pytest.raises(ValueError, match="Expected SparseVector values to be a list"):
        SparseVector(indices=[0, 1], values="not_a_list")
    with pytest.raises(
        ValueError, match="indices and values must have the same length"
    ):
        SparseVector(indices=[0, 1, 2], values=[0.1, 0.2])
    with pytest.raises(ValueError, match="SparseVector indices must be integers"):
        SparseVector(indices=[0, "not_int", 2], values=[0.1, 0.2, 0.3])
    with pytest.raises(ValueError, match="SparseVector indices must be non-negative"):
        SparseVector(indices=[0, -1, 2], values=[0.1, 0.2, 0.3])
    with pytest.raises(ValueError, match="SparseVector values must be numbers"):
        SparseVector(indices=[0, 1, 2], values=[0.1, "not_number", 0.3])
    with pytest.raises(ValueError, match="SparseVector indices must be integers"):
        SparseVector(indices=[0.0, 1.0, 2.0], values=[0.1, 0.2, 0.3])
    SparseVector(indices=[0, 1, 2], values=[1, 2, 3])
    SparseVector(indices=[0, 1, 2], values=[1, 2.5, 3])
    SparseVector(indices=[100, 1000, 10000], values=[0.1, 0.2, 0.3])
    with pytest.raises(ValueError, match="SparseVector values must be numbers"):
        SparseVector(indices=[0, 1], values=[0.1, None])
    with pytest.raises(ValueError, match="SparseVector indices must be integers"):
        SparseVector(indices=[0, None], values=[0.1, 0.2])
    SparseVector(indices=[42], values=[3.14])
    SparseVector(indices=[0, 1], values=[True, False])
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        SparseVector(indices=[0, 2, 1], values=[0.1, 0.2, 0.3])
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        SparseVector(indices=[0, 1, 1, 2], values=[0.1, 0.2, 0.3, 0.4])
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        SparseVector(indices=[5, 3, 1], values=[0.5, 0.3, 0.1])


def test_sparse_vector_in_metadata_validation():
    from chromadb.api.types import validate_metadata
    from chromadb.base_types import SparseVector

    sparse_vector_1 = SparseVector(indices=[0, 2, 5], values=[0.1, 0.5, 0.9])
    sparse_vector_2 = SparseVector(indices=[1, 3, 4], values=[0.2, 0.4, 0.6])
    metadata_1 = {
        "text": "document 1",
        "sparse_embedding": sparse_vector_1,
        "score": 0.5,
    }
    metadata_2 = {
        "text": "document 2",
        "sparse_embedding": sparse_vector_2,
        "score": 0.8,
    }
    validate_metadata(metadata_1)
    validate_metadata(metadata_2)
    metadata_empty = {
        "text": "empty sparse",
        "sparse_vec": SparseVector(indices=[], values=[]),
    }
    validate_metadata(metadata_empty)
    with pytest.raises(
        ValueError, match="indices and values must have the same length"
    ):
        invalid_metadata = {
            "text": "invalid",
            "sparse_embedding": SparseVector(indices=[0, 1], values=[0.1]),
        }
    invalid_metadata_2 = {
        "text": "missing indices",
        "sparse_embedding": {"values": [0.1, 0.2]},
    }
    with pytest.raises(
        ValueError,
        match="Expected metadata value to be a str, int, float, bool, SparseVector, list, or None",
    ):
        validate_metadata(invalid_metadata_2)
    with pytest.raises(ValueError, match="SparseVector indices must be non-negative"):
        invalid_metadata_3 = {
            "text": "negative index",
            "sparse_embedding": SparseVector(
                indices=[0, -1, 2], values=[0.1, 0.2, 0.3]
            ),
        }
    with pytest.raises(ValueError, match="SparseVector values must be numbers"):
        invalid_metadata_4 = {
            "text": "non-numeric value",
            "sparse_embedding": SparseVector(
                indices=[0, 1], values=[0.1, "not_a_number"]
            ),
        }
    metadata_multiple = {
        "text": "multiple sparse vectors",
        "sparse_1": SparseVector(indices=[0, 1], values=[0.1, 0.2]),
        "sparse_2": SparseVector(indices=[2, 3, 4], values=[0.3, 0.4, 0.5]),
        "regular_field": 42,
    }
    validate_metadata(metadata_multiple)
    metadata_nested = {
        "config": "some_config",
        "sparse_vector": {"indices": [0, 1, 2], "values": [1.0, 2.0, 3.0]},
    }
    with pytest.raises(
        ValueError,
        match="Expected metadata value to be a str, int, float, bool, SparseVector, list, or None",
    ):
        validate_metadata(metadata_nested)
    large_sparse = SparseVector(
        indices=list(range(1000)),
        values=[float(i) * 0.001 for i in range(1000)],
    )
    metadata_large = {"text": "large sparse", "large_sparse_vec": large_sparse}
    validate_metadata(metadata_large)


def test_sparse_vector_dict_format_normalization():
    from chromadb.api.types import normalize_metadata, validate_metadata, TYPE_KEY, SPARSE_VECTOR_TYPE_VALUE
    from chromadb.base_types import SparseVector

    metadata_dict_format = {
        "text": "test document",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 2, 5],
            "values": [1.0, 2.0, 3.0],
        },
    }
    normalized = normalize_metadata(metadata_dict_format)
    assert isinstance(normalized["sparse"], SparseVector)
    assert normalized["sparse"].indices == [0, 2, 5]
    assert normalized["sparse"].values == [1.0, 2.0, 3.0]
    validate_metadata(normalized)
    sparse_instance = SparseVector(indices=[1, 3, 4], values=[0.5, 1.5, 2.5])
    metadata_instance_format = {
        "text": "test document",
        "sparse": sparse_instance,
    }
    normalized2 = normalize_metadata(metadata_instance_format)
    assert normalized2["sparse"] is sparse_instance
    validate_metadata(normalized2)
    metadata_unsorted = {
        "text": "unsorted",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [5, 0, 2],
            "values": [3.0, 1.0, 2.0],
        },
    }
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        normalize_metadata(metadata_unsorted)
    metadata_duplicates = {
        "text": "duplicates",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 2, 2],
            "values": [1.0, 2.0, 3.0],
        },
    }
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        normalize_metadata(metadata_duplicates)
    metadata_negative = {
        "text": "negative",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [-1, 0, 2],
            "values": [1.0, 2.0, 3.0],
        },
    }
    with pytest.raises(ValueError, match="indices must be non-negative"):
        normalize_metadata(metadata_negative)
    metadata_mismatch = {
        "text": "mismatch",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 2],
            "values": [1.0, 2.0, 3.0],
        },
    }
    with pytest.raises(
        ValueError, match="indices and values must have the same length"
    ):
        normalize_metadata(metadata_mismatch)
    metadata_regular_dict = {
        "text": "regular",
        "config": {"key": "value"},
    }
    normalized3 = normalize_metadata(metadata_regular_dict)
    assert isinstance(normalized3["config"], dict)
    assert normalized3["config"]["key"] == "value"
    metadata_empty = {
        "text": "empty",
        "sparse": {TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE, "indices": [], "values": []},
    }
    normalized4 = normalize_metadata(metadata_empty)
    assert isinstance(normalized4["sparse"], SparseVector)
    assert normalized4["sparse"].indices == []
    assert normalized4["sparse"].values == []
    metadata_multiple = {
        "sparse1": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 1],
            "values": [1.0, 2.0],
        },
        "sparse2": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [2, 3],
            "values": [3.0, 4.0],
        },
        "regular": 42,
    }
    normalized5 = normalize_metadata(metadata_multiple)
    assert isinstance(normalized5["sparse1"], SparseVector)
    assert isinstance(normalized5["sparse2"], SparseVector)
    assert normalized5["regular"] == 42


def test_sparse_vector_dict_format_in_record_set():
    from chromadb.api.types import (
        normalize_insert_record_set,
        validate_insert_record_set,
        TYPE_KEY,
        SPARSE_VECTOR_TYPE_VALUE,
    )
    from chromadb.base_types import SparseVector

    record_set = normalize_insert_record_set(
        ids=["doc1", "doc2", "doc3"],
        embeddings=None,
        metadatas=[
            {
                "text": "test1",
                "sparse": {
                    TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
                    "indices": [0, 2],
                    "values": [1.0, 2.0],
                },
            },
            {
                "text": "test2",
                "sparse": SparseVector(indices=[1, 3], values=[1.5, 2.5]),
            },
            {"text": "test3"},
        ],
        documents=["doc one", "doc two", "doc three"],
    )
    assert isinstance(record_set["metadatas"][0]["sparse"], SparseVector)
    assert isinstance(record_set["metadatas"][1]["sparse"], SparseVector)
    assert "sparse" not in record_set["metadatas"][2]
    validate_insert_record_set(record_set)
    assert record_set["metadatas"][0]["sparse"].indices == [0, 2]
    assert record_set["metadatas"][0]["sparse"].values == [1.0, 2.0]
    assert record_set["metadatas"][1]["sparse"].indices == [1, 3]
    assert record_set["metadatas"][1]["sparse"].values == [1.5, 2.5]


def _is_python_local_segment(client):
    settings = client.get_settings()
    return (
        settings.chroma_api_impl == "chromadb.api.segment.SegmentAPI"
        and settings.chroma_segment_manager_impl
        == "chromadb.segment.impl.manager.local.LocalSegmentManager"
    )
