import pytest
import numpy as np

from chromadb.test.api.utils import (
    batch_records, minimal_records, bad_dimensionality_query, operator_records, records, contains_records
    )

def test_get_nearest_neighbors(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)

    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_embeddings=[1.1, 2.3, 3.2],
        n_results=1,
        where={},
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
        where={},
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
        where={},
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 2
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None
            
def test_dimensionality_validation_query(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_validation_query")
    collection.add(**minimal_records)

    with pytest.raises(Exception) as e:
        collection.query(**bad_dimensionality_query)
    assert "dimensionality" in str(e.value)

def test_query_document_valid_operators(client):
    client.reset()
    collection = client.create_collection("test_where_valid_operators")
    collection.add(**operator_records)

    with pytest.raises(ValueError, match="where document"):
        collection.query(query_embeddings=[0, 0, 0], where_document={"$contains": 2})
        
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
    
# make sure query results are returned in the right order
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
            where={},
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
            where={},
            include=["embeddings", "documents", "metadatas", "distances"],
        )
    assert "int" in str(exc.value)
    assert exc.type == ValueError
    
# test to make sure query error on invalid embeddings input
def test_query_invalid_embeddings(client):
    client.reset()
    collection = client.create_collection("test_invalid_embeddings")

    # Query with invalid embeddings
    with pytest.raises(ValueError) as e:
        collection.query(
            query_embeddings=[["1.1", "2.3", "3.2"]],
            n_results=1,
        )
    assert "embedding" in str(e.value)
    
def test_get_nearest_neighbors_where_n_results_more_than_element(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**records)

    includes = ["embeddings", "documents", "metadatas", "distances"]
    results = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=5,
        where={},
        include=includes,
    )
    for key in results.keys():
        if key in includes or key == "ids":
            assert len(results[key][0]) == 2
        elif key == "included":
            assert set(results[key]) == set(includes)
        else:
            assert results[key] is None 