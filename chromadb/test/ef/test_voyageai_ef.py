import os
import pytest
from chromadb.utils.embedding_functions.voyageai_embedding_function import (
    VoyageAIEmbeddingFunction,
)

voyageai = pytest.importorskip("voyageai", reason="voyageai not installed")


def test_with_embedding_dimensions() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-3.5",
        dimensions=2048,
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 2048


def test_with_multimodal_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_multimodal_image_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_multimodal_mixed_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_contextual_embedding() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-context-3",
        dimensions=2048,
    )
    embeddings = ef(["hello world", "in chroma"])
    assert embeddings is not None
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 2048
