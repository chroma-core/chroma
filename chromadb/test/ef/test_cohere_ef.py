import os
from typing import Any, List
from unittest.mock import MagicMock

import numpy as np
import pytest

from chromadb.utils.embedding_functions.cohere_embedding_function import (
    CohereEmbeddingFunction,
)

cohere = pytest.importorskip("cohere", reason="cohere not installed")
pytest.importorskip("PIL", reason="pillow not installed")


def _mock_client(dim: int = 4) -> MagicMock:
    def embed(**kwargs: Any) -> Any:
        n = len(kwargs.get("texts") or kwargs.get("images") or [])
        response = MagicMock()
        response.embeddings.float_ = [[0.1] * dim for _ in range(n)]
        return response

    client = MagicMock()
    client.embed.side_effect = embed
    return client


@pytest.fixture
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COHERE_API_KEY", raising=False)
    monkeypatch.setenv("CHROMA_COHERE_API_KEY", "test-key")


def test_defaults_to_embed_v5_pro(api_key: None) -> None:
    ef = CohereEmbeddingFunction()
    assert ef.model_name == "embed-v5.0-pro"
    assert ef.get_config() == {
        "api_key_env_var": "CHROMA_COHERE_API_KEY",
        "model_name": "embed-v5.0-pro",
    }
    CohereEmbeddingFunction.validate_config(ef.get_config())


def test_documents_and_queries_use_correct_input_types(api_key: None) -> None:
    ef = CohereEmbeddingFunction(query_model_name="embed-v5.0-fast")
    ef.client = _mock_client()

    docs = ef(["a document"])
    queries = ef.embed_query(["a query"])

    assert len(docs) == 1 and isinstance(docs[0], np.ndarray)
    assert len(queries) == 1

    doc_call, query_call = (c.kwargs for c in ef.client.embed.call_args_list)
    assert doc_call["input_type"] == "search_document"
    assert doc_call["model"] == "embed-v5.0-pro"
    assert doc_call["embedding_types"] == ["float"]
    assert query_call["input_type"] == "search_query"
    assert query_call["model"] == "embed-v5.0-fast"


def test_output_dimension_is_sent_and_persisted(api_key: None) -> None:
    ef = CohereEmbeddingFunction(output_dimension=1024)
    ef.client = _mock_client()
    ef(["a document"])

    assert ef.client.embed.call_args.kwargs["output_dimension"] == 1024
    config = ef.get_config()
    assert config["output_dimension"] == 1024
    CohereEmbeddingFunction.validate_config(config)

    rebuilt = CohereEmbeddingFunction.build_from_config(config)
    assert isinstance(rebuilt, CohereEmbeddingFunction)
    assert rebuilt.output_dimension == 1024


def test_rejects_query_model_from_another_space(api_key: None) -> None:
    with pytest.raises(ValueError, match="share an embedding space"):
        CohereEmbeddingFunction(
            model_name="embed-v5.0-pro", query_model_name="embed-english-v3.0"
        )


def test_legacy_config_still_builds(api_key: None) -> None:
    legacy = {"api_key_env_var": "CHROMA_COHERE_API_KEY", "model_name": "embed-english-v3.0"}
    CohereEmbeddingFunction.validate_config(legacy)
    ef = CohereEmbeddingFunction.build_from_config(legacy)
    assert ef.get_config() == legacy


def test_js_client_config_validates() -> None:
    # Shape written by the @chroma-core/cohere JS package.
    CohereEmbeddingFunction.validate_config(
        {
            "api_key_env_var": "COHERE_API_KEY",
            "model_name": "embed-v5.0-pro",
            "input_type": "search_document",
            "image": False,
        }
    )


def test_config_updates(api_key: None) -> None:
    ef = CohereEmbeddingFunction()
    old = ef.get_config()
    with pytest.raises(ValueError):
        ef.validate_config_update(old, {"model_name": "embed-v5.0-fast"})
    with pytest.raises(ValueError):
        ef.validate_config_update(old, {"output_dimension": 512})
    # Switching the query model within the shared space is allowed.
    ef.validate_config_update(old, {"query_model_name": "embed-v5.0-fast"})


def test_live_embeddings() -> None:
    if os.environ.get("COHERE_API_KEY") is None:
        pytest.skip("COHERE_API_KEY not set")
    ef = CohereEmbeddingFunction(
        query_model_name="embed-v5.0-fast", output_dimension=1024
    )
    docs: List[Any] = ef(["hello world", "goodbye world"])
    queries: List[Any] = ef.embed_query(["hello"])
    assert len(docs) == 2 and len(queries) == 1
    assert len(docs[0]) == 1024 and len(queries[0]) == 1024
