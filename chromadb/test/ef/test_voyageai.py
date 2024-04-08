import os

import pytest

from chromadb.utils.embedding_functions import VoyageAIEmbeddingFunction


def test_voyage() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    ef = VoyageAIEmbeddingFunction(api_key=os.environ.get("VOYAGEAI_API_KEY", ""))
    embeddings = ef(["test doc"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


def test_voyage_input_type_query() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGEAI_API_KEY", ""), input_type="query"
    )
    embeddings = ef(["test doc"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


def test_voyage_input_type_document() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGEAI_API_KEY", ""), input_type="document"
    )
    embeddings = ef(["test doc"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


def test_voyage_model() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGEAI_API_KEY", ""), model_name="voyage-code-2"
    )
    embeddings = ef(["def test():\n    return 1"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


def test_voyage_truncation_default() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    ef = VoyageAIEmbeddingFunction(api_key=os.environ.get("VOYAGEAI_API_KEY", ""))
    embeddings = ef(["this is a test-message" * 10000])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


def test_voyage_truncation_enabled() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGEAI_API_KEY", ""), truncation=True
    )
    embeddings = ef(["this is a test-message" * 10000])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


def test_voyage_truncation_disabled() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGEAI_API_KEY", ""), truncation=False
    )
    with pytest.raises(Exception, match="your batch has too many tokens"):
        ef(["this is a test-message" * 10000])


def test_voyage_no_api_key() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    with pytest.raises(ValueError, match="Please provide a VoyageAI API key"):
        VoyageAIEmbeddingFunction(api_key=None)  # type: ignore


def test_voyage_max_batch_size_exceeded_in_init() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    with pytest.raises(ValueError, match="The maximum batch size supported is"):
        VoyageAIEmbeddingFunction(api_key="dummy", max_batch_size=99999999)


def test_voyage_max_batch_size_exceeded_in_call() -> None:
    if "VOYAGEAI_API_KEY" not in os.environ:
        pytest.skip("VOYAGEAI_API_KEY not set, not going to test VoyageAI EF.")
    ef = VoyageAIEmbeddingFunction(api_key="dummy", max_batch_size=1)
    with pytest.raises(ValueError, match="The maximum batch size supported is"):
        ef(["test doc"] * 2)
