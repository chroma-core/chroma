from typing import Tuple
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from chromadb.utils.embedding_functions.cohere_embedding_function import (
    CohereEmbeddingFunction,
)

pytest.importorskip("cohere", reason="cohere not installed")
pytest.importorskip("PIL", reason="Pillow not installed")


@pytest.fixture
def cohere_ef(
    monkeypatch: pytest.MonkeyPatch,
) -> Tuple[CohereEmbeddingFunction, MagicMock]:
    monkeypatch.delenv("COHERE_API_KEY", raising=False)
    monkeypatch.setenv("CHROMA_COHERE_API_KEY", "test-key")
    with patch("cohere.Client") as client:
        client.return_value.embed.return_value.embeddings = [[0.25, 0.5]]
        ef = CohereEmbeddingFunction(model_name="embed-english-v3.0")
    return ef, client.return_value


@pytest.mark.parametrize("is_query", [False, True])
def test_text_input_type(
    cohere_ef: Tuple[CohereEmbeddingFunction, MagicMock], is_query: bool
) -> None:
    ef, client = cohere_ef
    embed = ef.embed_query if is_query else ef.__call__

    embeddings = embed(["hello world"])

    client.embed.assert_called_once_with(
        texts=["hello world"],
        model="embed-english-v3.0",
        input_type="search_query" if is_query else "search_document",
    )
    np.testing.assert_array_equal(embeddings, [[0.25, 0.5]])
    assert embeddings[0].dtype == np.float32


@pytest.mark.parametrize("is_query", [False, True])
def test_image_input_type(
    cohere_ef: Tuple[CohereEmbeddingFunction, MagicMock], is_query: bool
) -> None:
    ef, client = cohere_ef
    embed = ef.embed_query if is_query else ef.__call__

    embeddings = embed([np.zeros((2, 2, 3), dtype=np.uint8)])

    kwargs = client.embed.call_args.kwargs
    assert kwargs["input_type"] == "image"
    assert kwargs["model"] == "embed-english-v3.0"
    assert len(kwargs["images"]) == 1
    assert kwargs["images"][0].startswith("data:image/png;base64,")
    assert "texts" not in kwargs
    np.testing.assert_array_equal(embeddings, [[0.25, 0.5]])
    assert embeddings[0].dtype == np.float32


@pytest.mark.parametrize("is_query", [False, True])
def test_mixed_input_rejected(
    cohere_ef: Tuple[CohereEmbeddingFunction, MagicMock], is_query: bool
) -> None:
    ef, client = cohere_ef
    embed = ef.embed_query if is_query else ef.__call__

    with pytest.raises(ValueError, match="mix of text documents and images"):
        embed(["hello world", np.zeros((2, 2, 3), dtype=np.uint8)])

    client.embed.assert_not_called()


def test_query_does_not_change_document_input_type(
    cohere_ef: Tuple[CohereEmbeddingFunction, MagicMock],
) -> None:
    ef, client = cohere_ef
    config = ef.get_config()

    ef.embed_query(["query"])
    ef(["document"])

    assert [call.kwargs["input_type"] for call in client.embed.call_args_list] == [
        "search_query",
        "search_document",
    ]
    assert ef.get_config() == config
