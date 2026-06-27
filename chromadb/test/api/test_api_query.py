# type: ignore
import numpy as np
import pytest

from chromadb.api.types import QueryResult

batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}

records = {
    "embeddings": [[0, 0, 0], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
    "documents": ["this document is first", "this document is second"],
}

operator_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}

contains_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "documents": ["this is doc1 and it's great!", "doc2 is also great!"],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}


def test_get_nearest_neighbors(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_embeddings=[1.1, 2.3, 3.2],
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None
    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None
    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2], [0.1, 2.3, 4.5]],
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 2
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


def test_get_nearest_neighbors_where_n_results_more_than_element(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**records)
    includes = ["embeddings", "documents", "metadatas", "distances"]
    results = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=5,
        include=includes,
    )
    for key in results.keys():
        if key in includes or key == "ids":
            assert len(results[key][0]) == 2
        elif key == "included":
            assert set(results[key]) == set(includes)
        else:
            assert results[key] is None


def test_query_document_valid_operators(client):
    client.reset()
    collection = client.create_collection("test_where_valid_operators")
    collection.add(**operator_records)
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$lt": {"$nested": 2}})
    with pytest.raises(ValueError, match="where document"):
        collection.query(query_embeddings=[0, 0, 0], where_document={"$contains": 2})
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$contains": []})
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$contains": {"text": "hello"}})
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$not_contains": {"text": "hello"}})
    with pytest.raises(ValueError):
        collection.get(where_document={"$and": {"$unsupported": "doc"}})
    with pytest.raises(ValueError):
        collection.get(
            where_document={"$or": [{"$unsupported": "doc"}, {"$unsupported": "doc"}]}
        )
    with pytest.raises(ValueError):
        collection.get(where_document={"$or": [{"$contains": "doc"}]})
    with pytest.raises(ValueError):
        collection.get(where_document={"$or": []})
    with pytest.raises(ValueError):
        collection.get(
            where_document={
                "$or": [{"$and": [{"$contains": "doc"}]}, {"$contains": "doc"}]
            }
        )


def test_get_where_document(client):
    client.reset()
    collection = client.create_collection("test_get_where_document")
    collection.add(**contains_records)
    items = collection.get(where_document={"$contains": "doc1"})
    assert len(items["metadatas"]) == 1
    items = collection.get(where_document={"$contains": "great"})
    assert len(items["metadatas"]) == 2
    items = collection.get(where_document={"$contains": "bad"})
    assert len(items["metadatas"]) == 0


def test_query_where_document(client):
    client.reset()
    collection = client.create_collection("test_query_where_document")
    collection.add(**contains_records)
    items = collection.query(
        query_embeddings=[1, 0, 0], where_document={"$contains": "doc1"}, n_results=1
    )
    assert len(items["metadatas"][0]) == 1
    items = collection.query(
        query_embeddings=[0, 0, 0], where_document={"$contains": "great"}, n_results=2
    )
    assert len(items["metadatas"][0]) == 2
    with pytest.raises(Exception) as e:
        items = collection.query(
            query_embeddings=[0, 0, 0], where_document={"$contains": "bad"}, n_results=1
        )
        assert "datapoints" in str(e.value)


def test_query_include(client):
    client.reset()
    collection = client.create_collection("test_query_include")
    collection.add(**records)
    include = ["metadatas", "documents", "distances"]
    items = collection.query(
        query_embeddings=[0, 0, 0],
        include=include,
        n_results=1,
    )
    assert items["embeddings"] is None
    assert items["ids"][0][0] == "id1"
    assert items["metadatas"][0][0]["int_value"] == 1
    assert set(items["included"]) == set(include)
    include = ["embeddings", "documents", "distances"]
    items = collection.query(
        query_embeddings=[0, 0, 0],
        include=include,
        n_results=1,
    )
    assert items["metadatas"] is None
    assert items["ids"][0][0] == "id1"
    assert set(items["included"]) == set(include)
    items = collection.query(
        query_embeddings=[[0, 0, 0], [1, 2, 1.2]],
        include=[],
        n_results=2,
    )
    assert items["documents"] is None
    assert items["metadatas"] is None
    assert items["embeddings"] is None
    assert items["distances"] is None
    assert items["ids"][0][0] == "id1"
    assert items["ids"][0][1] == "id2"


def test_query_order(client):
    client.reset()
    collection = client.create_collection("test_query_order")
    collection.add(**records)
    items = collection.query(
        query_embeddings=[1.2, 2.24, 3.2],
        include=["metadatas", "documents", "distances"],
        n_results=2,
    )
    assert items["documents"][0][0] == "this document is second"
    assert items["documents"][0][1] == "this document is first"


def test_invalid_n_results_param(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**records)
    with pytest.raises(TypeError) as exc:
        collection.query(
            query_embeddings=[[1.1, 2.3, 3.2]],
            n_results=-1,
            include=["embeddings", "documents", "metadatas", "distances"],
        )
    assert "Number of requested results -1, cannot be negative, or zero." in str(
        exc.value
    )
    assert exc.type == TypeError
    with pytest.raises(ValueError) as exc:
        collection.query(
            query_embeddings=[[1.1, 2.3, 3.2]],
            n_results="one",
            include=["embeddings", "documents", "metadatas", "distances"],
        )
    assert "int" in str(exc.value)
    assert exc.type == ValueError


def test_query_id_filtering_small_dataset(client):
    client.reset()
    collection = client.create_collection("test_query_id_filtering_small")
    num_vectors = 100
    dim = 512
    small_records = np.random.rand(100, 512).astype(np.float32).tolist()
    ids = [f"{i}" for i in range(num_vectors)]
    collection.add(
        embeddings=small_records,
        ids=ids,
    )
    query_ids = [f"{i}" for i in range(0, num_vectors, 10)]
    query_embedding = np.random.rand(dim).astype(np.float32).tolist()
    results = collection.query(
        query_embeddings=query_embedding,
        ids=query_ids,
        n_results=num_vectors,
        include=[],
    )
    all_returned_ids = [item for sublist in results["ids"] for item in sublist]
    assert all(id in query_ids for id in all_returned_ids)


def test_query_id_filtering_medium_dataset(client):
    client.reset()
    collection = client.create_collection("test_query_id_filtering_medium")
    num_vectors = 1000
    dim = 512
    medium_records = np.random.rand(num_vectors, dim).astype(np.float32).tolist()
    ids = [f"{i}" for i in range(num_vectors)]
    collection.add(
        embeddings=medium_records,
        ids=ids,
    )
    query_ids = [f"{i}" for i in range(0, num_vectors, 10)]
    query_embedding = np.random.rand(dim).astype(np.float32).tolist()
    results = collection.query(
        query_embeddings=query_embedding,
        ids=query_ids,
        n_results=num_vectors,
        include=[],
    )
    all_returned_ids = [item for sublist in results["ids"] for item in sublist]
    assert all(id in query_ids for id in all_returned_ids)
    multi_query_embeddings = [
        np.random.rand(dim).astype(np.float32).tolist() for _ in range(3)
    ]
    multi_results = collection.query(
        query_embeddings=multi_query_embeddings,
        ids=query_ids,
        n_results=10,
        include=[],
    )
    for result_set in multi_results["ids"]:
        assert all(id in query_ids for id in result_set)


def test_query_id_filtering_e2e(client):
    client.reset()
    collection = client.create_collection("test_query_id_filtering_e2e")
    dim = 512
    num_vectors = 100
    embeddings = np.random.rand(num_vectors, dim).astype(np.float32).tolist()
    ids = [f"{i}" for i in range(num_vectors)]
    metadatas = [{"index": i} for i in range(num_vectors)]
    collection.add(
        embeddings=embeddings,
        ids=ids,
        metadatas=metadatas,
    )
    ids_to_delete = [f"{i}" for i in range(10, 30)]
    collection.delete(ids=ids_to_delete)
    ids_to_upsert_existing = [f"{i}" for i in range(30, 50)]
    new_num_vectors = num_vectors + 20
    ids_to_upsert_new = [f"{i}" for i in range(num_vectors, new_num_vectors)]
    upsert_embeddings = (
        np.random.rand(len(ids_to_upsert_existing) + len(ids_to_upsert_new), dim)
        .astype(np.float32)
        .tolist()
    )
    upsert_metadatas = [
        {"index": i, "upserted": True} for i in range(len(upsert_embeddings))
    ]
    collection.upsert(
        embeddings=upsert_embeddings,
        ids=ids_to_upsert_existing + ids_to_upsert_new,
        metadatas=upsert_metadatas,
    )
    valid_query_ids = (
        [f"{i}" for i in range(5, 10)]
        + [f"{i}" for i in range(35, 45)]
        + [f"{i}" for i in range(num_vectors + 5, num_vectors + 15)]
    )
    includes = ["metadatas"]
    query_embedding = np.random.rand(dim).astype(np.float32).tolist()
    results = collection.query(
        query_embeddings=query_embedding,
        ids=valid_query_ids,
        n_results=new_num_vectors,
        include=includes,
    )
    all_returned_ids = [item for sublist in results["ids"] for item in sublist]
    assert all(id in valid_query_ids for id in all_returned_ids)
    for result_index, id_list in enumerate(results["ids"]):
        for item_index, item_id in enumerate(id_list):
            if item_id in ids_to_upsert_existing or item_id in ids_to_upsert_new:
                assert results["metadatas"][result_index][item_index]["upserted"]
    upserted_id = ids_to_upsert_existing[0]
    results = collection.query(
        query_embeddings=query_embedding,
        ids=upserted_id,
        n_results=1,
        include=includes,
    )
    assert results["metadatas"][0][0]["upserted"]
    deleted_id = ids_to_delete[0]
    with pytest.raises(Exception) as error:
        collection.query(
            query_embeddings=query_embedding,
            ids=deleted_id,
            n_results=1,
            include=includes,
        )
    assert "Error finding id" in str(error.value)


def test_delete_where_document(client):
    client.reset()
    collection = client.create_collection("test_delete_where_document")
    collection.add(**contains_records)
    collection.delete(where_document={"$contains": "doc1"})
    assert collection.count() == 1
    collection.delete(where_document={"$contains": "bad"})
    assert collection.count() == 1
    collection.delete(where_document={"$contains": "great"})
    assert collection.count() == 0
