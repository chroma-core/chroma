import math

import pytest

from chromadb.utils.embedding_functions.chroma_bm25_embedding_function import (
    DEFAULT_CHROMA_BM25_STOPWORDS,
    ChromaBm25EmbeddingFunction,
)


def _is_sorted(values: list[int]) -> bool:
    return all(values[i] >= values[i - 1] for i in range(1, len(values)))


def test_comprehensive_tokenization_matches_reference() -> None:
    embedder = ChromaBm25EmbeddingFunction()
    embedding = embedder(
        [
            "Usain Bolt's top speed reached ~27.8 mph (44.72 km/h)",
        ]
    )[0]

    expected_indices = [
        230246813,
        395514983,
        458027949,
        488165615,
        729632045,
        734978415,
        997512866,
        1114505193,
        1381820790,
        1501587190,
        1649421877,
        1837285388,
    ]
    expected_value = 1.6391153

    assert embedding.indices == expected_indices
    for value in embedding.values:
        assert value == pytest.approx(expected_value, abs=1e-5)


def test_matches_rust_reference_values() -> None:
    embedder = ChromaBm25EmbeddingFunction()
    embedding = embedder(
        [
            "The   space-time   continuum   WARPS   near   massive   objects...",
        ]
    )[0]

    expected_indices = [
        90097469,
        519064992,
        737893654,
        1110755108,
        1950894484,
        2031641008,
        2058513491,
    ]
    expected_value = 1.660867

    assert embedding.indices == expected_indices
    for value in embedding.values:
        assert value == pytest.approx(expected_value, abs=1e-5)


def test_generates_embeddings_for_multiple_documents() -> None:
    embedder = ChromaBm25EmbeddingFunction()
    texts = [
        "Usain Bolt's top speed reached ~27.8 mph (44.72 km/h)",
        "The   space-time   continuum   WARPS   near   massive   objects...",
        "BM25 is great for sparse retrieval tasks",
    ]

    embeddings = embedder(texts)

    assert len(embeddings) == len(texts)
    for embedding in embeddings:
        assert embedding.indices
        assert len(embedding.indices) == len(embedding.values)
        assert _is_sorted(embedding.indices)
        for value in embedding.values:
            assert value > 0
            assert math.isfinite(value)


def test_embed_query_matches_call() -> None:
    embedder = ChromaBm25EmbeddingFunction()
    query = "retrieve BM25 docs"

    query_embedding = embedder.embed_query([query])[0]
    doc_embedding = embedder([query])[0]

    assert query_embedding.indices == doc_embedding.indices
    assert query_embedding.values == doc_embedding.values


def test_config_round_trip() -> None:
    embedder = ChromaBm25EmbeddingFunction()
    config = embedder.get_config()

    assert config["k"] == pytest.approx(1.2, abs=1e-9)
    assert config["b"] == pytest.approx(0.75, abs=1e-9)
    assert config["avg_doc_length"] == pytest.approx(256.0, abs=1e-9)
    assert config["token_max_length"] == 40
    assert "stopwords" not in config

    custom_stopwords = DEFAULT_CHROMA_BM25_STOPWORDS[:10]
    rebuilt = ChromaBm25EmbeddingFunction.build_from_config(
        {
            **config,
            "stopwords": custom_stopwords,
        }
    )

    rebuilt_config = rebuilt.get_config()
    assert rebuilt_config["stopwords"] == custom_stopwords
    assert rebuilt_config["token_max_length"] == config["token_max_length"]
    assert rebuilt_config["k"] == pytest.approx(config["k"], abs=1e-9)
    assert rebuilt_config["b"] == pytest.approx(config["b"], abs=1e-9)
    assert rebuilt_config["avg_doc_length"] == pytest.approx(
        config["avg_doc_length"], abs=1e-9
    )


def test_validate_config_update_rejects_unknown_keys() -> None:
    embedder = ChromaBm25EmbeddingFunction()

    with pytest.raises(ValueError):
        embedder.validate_config_update(embedder.get_config(), {"unknown": 123})


def test_validate_config_update_allows_known_keys() -> None:
    embedder = ChromaBm25EmbeddingFunction()

    embedder.validate_config_update(
        embedder.get_config(), {"k": 1.1, "stopwords": ["custom"]}
    )
