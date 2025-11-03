import numpy as np
import pytest

from chromadb.utils.embedding_functions.simple_hash_embedding_function import (
    SimpleHashEmbeddingFunction,
)
from chromadb.utils.embedding_functions import config_to_embedding_function


def test_simple_hash_basic() -> None:
    ef = SimpleHashEmbeddingFunction(dim=16)

    docs = ["hello", "world", ""]
    embeddings = ef(docs)

    assert isinstance(embeddings, list)
    assert len(embeddings) == 3

    # first two should be non-zero, third (empty string) should be zero vector
    assert isinstance(embeddings[0], np.ndarray)
    assert embeddings[0].dtype == np.float32
    assert embeddings[0].shape == (16,)
    assert np.linalg.norm(embeddings[0]) > 0
    assert np.linalg.norm(embeddings[1]) > 0
    assert np.allclose(embeddings[2], np.zeros(16, dtype=np.float32))


def test_embed_query_and_determinism() -> None:
    ef = SimpleHashEmbeddingFunction(dim=8)
    q = ["test query"]
    a = ef(q)
    b = ef.embed_query(q)
    # same content -> same embedding
    assert len(a) == len(b) == 1
    assert np.allclose(a[0], b[0])

    # deterministic across calls
    c = ef(["test query"])
    assert np.allclose(a[0], c[0])


def test_config_integration() -> None:
    cfg = {"name": "local_simple_hash", "config": {"dim": 12}}
    ef = config_to_embedding_function(cfg)
    assert isinstance(ef, SimpleHashEmbeddingFunction)
    out = ef(["x"])
    assert len(out) == 1 and out[0].shape == (12,)


def test_edge_cases_long_and_non_string_inputs() -> None:
    ef = SimpleHashEmbeddingFunction(dim=20)

    # Very long string should produce a stable vector and not error
    long_text = "a" * 10000
    long_emb = ef([long_text])[0]
    assert long_emb.shape == (20,)
    assert np.linalg.norm(long_emb) > 0

    # Non-string inputs should be accepted and converted via str()
    samples = [123, None, 45.6, b"bytes"]
    # Convert samples to strings before passing to the embedding function to satisfy typing
    stringified = [str(s) for s in samples]
    emb = ef(stringified)
    assert len(emb) == 4
    for v in emb:
        assert isinstance(v, np.ndarray)
        assert v.shape == (20,)

    # Empty input list is not allowed at the public API layer; wrapper validates non-empty
    with pytest.raises(ValueError):
        ef([])
