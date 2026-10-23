"""Public embedded-client sparse metadata transport and Search regression."""
from pathlib import Path
from typing import Any, cast

from chromadb.api.client import Client

import chromadb
import pytest
from chromadb.errors import InvalidArgumentError
from chromadb import K, Knn, Search
from chromadb.api.types import Schema, SparseVector, SparseVectorIndexConfig
from chromadb.config import Settings


def test_sparse_metadata_public_roundtrip(tmp_path: Path) -> None:
    client = chromadb.PersistentClient(
        path=str(tmp_path),
        settings=Settings(_env_file=None, anonymized_telemetry=False),  # type: ignore[call-arg]
    )
    try:
        schema = Schema()
        schema.create_index(key="sparse", config=SparseVectorIndexConfig(bm25=False))
        collection = client.create_collection(
            "sparse-roundtrip", schema=schema, embedding_function=None
        )
        vector = SparseVector(indices=[1], values=[2.0], labels=["token"])
        empty = SparseVector(indices=[], values=[], labels=[])
        collection.add(
            ids=["a", "b"],
            embeddings=[[1.0, 0.0], [0.0, 1.0]],
            metadatas=[
                {"sparse": vector, "scalar": True, "array": [1, 2]},
                {"sparse": empty},
            ],
        )
        metadata = collection.get(ids=["a", "b"])["metadatas"]
        assert metadata is not None
        assert metadata[0]["sparse"] == vector
        assert metadata[1]["sparse"] == empty
        assert metadata[0]["scalar"] is True
        assert metadata[0]["array"] == [1, 2]
        query = collection.query(query_embeddings=[[1.0, 0.0]], n_results=2)
        assert query["metadatas"] is not None
        assert query["metadatas"][0][0]["sparse"] == vector
        search = (
            Search()
            .rank(Knn(query=vector, key="sparse", limit=10))
            .select(K.SCORE, "sparse")
        )
        result = collection.search(search)
        assert result["ids"] == [["a", "b"]]
        assert result["scores"] == [[-3.0, 1.0]]
        assert result["metadatas"][0][0]["sparse"] == vector
        assert collection.search(search.where(K.ID.is_in(["b"])))["ids"] == [["b"]]
        assert collection.search(search.where(K.ID.not_in(["a"])))["ids"] == [["b"]]
        mixed = search.where({"$and": [{"#id": {"$eq": "a"}}, {"scalar": True}]})
        assert collection.search(mixed)["ids"] == [["a"]]
        with pytest.raises(NotImplementedError, match="Local Search supports"):
            collection.search(search.where({"#score": {"$eq": 0}}))
        unsupported_filters: list[dict[str, Any]] = [
            {"#id": {"$gt": "a"}},
            {"#id": {"$eq": 1}},
        ]
        for unsupported_filter in unsupported_filters:
            with pytest.raises((ValueError, InvalidArgumentError, NotImplementedError)):
                collection.search(search.where(unsupported_filter))
        collection.update(ids=["a"], metadatas=[{"sparse": empty}])
        collection.upsert(
            ids=["b"], embeddings=[[0.0, 1.0]], metadatas=[{"sparse": vector}]
        )
        assert collection.search(search)["ids"] == [["b", "a"]]
        collection.update(ids=["b"], metadatas=[{"sparse": None}])
        assert collection.search(search)["ids"] == [["a"]]
        final = collection.get(ids=["b"])["metadatas"]
        assert final is not None
        assert final[0] is None or "sparse" not in final[0]
    finally:
        cast(Client, client)._system.stop()
