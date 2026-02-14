import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from chromadb import SparseVector
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


def test_multithreaded_usage() -> None:
    embedder = ChromaBm25EmbeddingFunction()
    base_texts = [
        """The gravitational wave background from massive black hole binaries emit bursts of
        gravitational waves at periapse. Such events may be directly resolvable in the Galactic
        centre. However, if the star does not spiral in, the emitted GWs are not resolvable for
        extra-galactic MBHs, but constitute a source of background noise. We estimate the power
        spectrum of this extreme mass ratio burst background.""",
        """Dynamics of planets in exoplanetary systems with multiple stars showing how the
        gravitational interactions between the stars and planets affect the orbital stability
        and long-term evolution of the planetary system architectures.""",
        """Diurnal Thermal Tides in a Non-rotating atmosphere with realistic heating profiles
        and temperature gradients that demonstrate the complex interplay between radiation
        and atmospheric dynamics in planetary atmospheres.""",
        """Intermittent turbulence, noise and waves in stellar atmospheres create complex
        patterns of energy transport and momentum deposition that influence the structure
        and evolution of stellar interiors and surfaces.""",
        """Superconductivity in quantum materials and condensed matter physics systems
        exhibiting novel quantum phenomena including topological phases, strongly correlated
        electron systems, and exotic superconducting pairing mechanisms.""",
        """Machine learning models require careful tuning of hyperparameters including learning
        rates, regularization coefficients, and architectural choices that demonstrate the
        complex interplay between optimization algorithms and model capacity.""",
        """Natural language processing enables text understanding through sophisticated
        algorithms that analyze semantic relationships, syntactic structures, and contextual
        information to extract meaningful representations from unstructured textual data.""",
        """Vector databases store high-dimensional embeddings efficiently using advanced
        indexing techniques including approximate nearest neighbor search algorithms that
        balance accuracy and computational efficiency for large-scale similarity search.""",
    ]
    texts = base_texts * 30

    num_threads = 10

    def process_single_text(text: str) -> SparseVector:
        return embedder([text])[0]

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_single_text, text) for text in texts]
        all_results = []
        for future in as_completed(futures):
            try:
                embedding = future.result()
                all_results.append(embedding)
            except Exception as e:
                pytest.fail(
                    f"Threading error detected: {type(e).__name__}: {e}. "
                    "This indicates the stemmer is not thread-safe when cached."
                )

    assert len(all_results) == len(texts)

    for embedding in all_results:
        assert embedding.indices
        assert len(embedding.indices) == len(embedding.values)
        assert _is_sorted(embedding.indices)
        for value in embedding.values:
            assert value > 0
            assert math.isfinite(value)
