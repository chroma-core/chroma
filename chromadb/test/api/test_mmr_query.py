"""End-to-end tests for Collection.max_marginal_relevance_query (MMR)."""

import inspect
from typing import List, Tuple
from uuid import uuid4

import chromadb
import numpy as np
import pytest

from chromadb.api.models.AsyncCollection import AsyncCollection
from chromadb.api.models.Collection import Collection
from chromadb.config import Settings


def _unit(vec: List[float]) -> List[float]:
    arr = np.asarray(vec, dtype=np.float32)
    return (arr / np.linalg.norm(arr)).tolist()


def _clustered_collection() -> Tuple[Collection, List[float]]:
    """A collection with two tight clusters near the query plus one outlier.

    The query points at cluster A, whose members are near-duplicates of each
    other. Plain top-k therefore returns five copies of essentially the same
    direction; MMR should be able to reach into cluster B and the outlier.
    """
    client = chromadb.Client(Settings(allow_reset=True, anonymized_telemetry=False))
    client.reset()
    collection = client.get_or_create_collection(
        name=f"mmr_{uuid4().hex}",
        metadata={"hnsw:space": "cosine"},
    )

    ids: List[str] = []
    embeddings: List[List[float]] = []
    for i in range(5):
        ids.append(f"a_{i}")
        embeddings.append(_unit([1.0, 0.02 * i, 0.0]))
    for i in range(5):
        ids.append(f"b_{i}")
        embeddings.append(_unit([0.9, 0.6, 0.02 * i]))
    ids.append("outlier")
    embeddings.append(_unit([0.7, 0.0, 0.7]))

    collection.add(ids=ids, embeddings=embeddings)
    query = _unit([1.0, 0.1, 0.0])
    return collection, query


def _groups(ids: List[str]) -> set:
    return {i.split("_", 1)[0] for i in ids}


def test_mmr_lambda_one_matches_plain_query() -> None:
    collection, query = _clustered_collection()

    plain = collection.query(query_embeddings=[query], n_results=5)["ids"][0]
    mmr = collection.max_marginal_relevance_query(
        query_embeddings=[query], n_results=5, fetch_k=11, lambda_mult=1.0
    )["ids"][0]

    # lambda=1.0 is pure relevance and must reproduce the plain top-k order.
    assert mmr == plain


def test_mmr_diversifies_results() -> None:
    collection, query = _clustered_collection()

    plain = collection.query(query_embeddings=[query], n_results=5)["ids"][0]
    mmr = collection.max_marginal_relevance_query(
        query_embeddings=[query], n_results=5, fetch_k=11, lambda_mult=0.3
    )["ids"][0]

    assert len(set(mmr)) == 5
    # Plain top-5 collapses into a single cluster; MMR must cover strictly more.
    assert len(_groups(mmr)) > len(_groups(plain))


def test_mmr_include_embeddings_roundtrip() -> None:
    collection, query = _clustered_collection()

    with_emb = collection.max_marginal_relevance_query(
        query_embeddings=[query],
        n_results=4,
        fetch_k=11,
        include=["documents", "distances", "embeddings"],
    )
    assert with_emb["embeddings"] is not None
    assert len(with_emb["embeddings"][0]) == 4
    assert "embeddings" in with_emb["included"]

    # When embeddings are not requested they must not leak into the result even
    # though MMR fetched them internally.
    without_emb = collection.max_marginal_relevance_query(
        query_embeddings=[query], n_results=4, fetch_k=11
    )
    assert without_emb["embeddings"] is None
    assert "embeddings" not in without_emb["included"]


def test_mmr_batch_queries_independent() -> None:
    collection, query = _clustered_collection()
    other = _unit([0.9, 0.6, 0.0])  # points at cluster B

    result = collection.max_marginal_relevance_query(
        query_embeddings=[query, other], n_results=3, fetch_k=11, lambda_mult=0.5
    )
    assert len(result["ids"]) == 2
    assert len(result["ids"][0]) == 3
    assert len(result["ids"][1]) == 3


def test_mmr_returns_n_results() -> None:
    collection, query = _clustered_collection()
    result = collection.max_marginal_relevance_query(
        query_embeddings=[query], n_results=6, fetch_k=8
    )
    assert len(result["ids"][0]) == 6


def test_mmr_fetch_k_clamped_to_n_results() -> None:
    collection, query = _clustered_collection()
    # fetch_k < n_results should be clamped up rather than truncating output.
    result = collection.max_marginal_relevance_query(
        query_embeddings=[query], n_results=5, fetch_k=2
    )
    assert len(result["ids"][0]) == 5


def test_mmr_invalid_arguments() -> None:
    collection, query = _clustered_collection()
    with pytest.raises(ValueError):
        collection.max_marginal_relevance_query(
            query_embeddings=[query], lambda_mult=1.5
        )
    with pytest.raises(ValueError):
        collection.max_marginal_relevance_query(query_embeddings=[query], n_results=0)


def test_async_collection_exposes_mmr_query_method() -> None:
    assert inspect.iscoroutinefunction(AsyncCollection.max_marginal_relevance_query)
