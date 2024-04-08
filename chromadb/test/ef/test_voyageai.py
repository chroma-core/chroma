import os

import pytest

from chromadb.utils.embedding_functions import VoyageAIEmbeddingFunction


@pytest.fixture(scope="function")
def remove_api_key():
    existing_api_key = None
    if "VOYAGE_API_KEY" in os.environ:
        existing_api_key = os.environ["VOYAGE_API_KEY"]
        print("removing key")
        del os.environ["VOYAGE_API_KEY"]
    yield
    if existing_api_key:
        print("setting kye")
        os.environ["VOYAGE_API_KEY"] = existing_api_key


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage() -> None:
    ef = VoyageAIEmbeddingFunction(api_key=os.environ.get("VOYAGE_API_KEY", ""))
    embeddings = ef(["test doc"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_input_type_query() -> None:
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGE_API_KEY", ""),
        input_type=VoyageAIEmbeddingFunction.InputType.QUERY,
    )
    embeddings = ef(["test doc"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_input_type_document() -> None:
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGE_API_KEY", ""),
        input_type=VoyageAIEmbeddingFunction.InputType.DOCUMENT,
    )
    embeddings = ef(["test doc"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_model() -> None:
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGE_API_KEY", ""), model_name="voyage-01"
    )
    embeddings = ef(["def test():\n    return 1"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_truncation_default() -> None:
    ef = VoyageAIEmbeddingFunction(api_key=os.environ.get("VOYAGE_API_KEY", ""))
    embeddings = ef(["this is a test-message" * 10000])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_truncation_enabled() -> None:
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGE_API_KEY", ""), truncation=True
    )
    embeddings = ef(["this is a test-message" * 10000])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_truncation_disabled() -> None:
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ.get("VOYAGE_API_KEY", ""), truncation=False
    )
    with pytest.raises(Exception, match="your batch has too many tokens"):
        ef(["this is a test-message" * 10000])


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_env_api_key() -> None:
    VoyageAIEmbeddingFunction()


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_no_api_key(remove_api_key) -> None:
    with pytest.raises(ValueError, match="Please provide a VoyageAI API key"):
        VoyageAIEmbeddingFunction(api_key=None)  # type: ignore


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_no_api_key_in_env(remove_api_key) -> None:
    with pytest.raises(ValueError, match="Please provide a VoyageAI API key"):
        VoyageAIEmbeddingFunction(api_key=None)  # type: ignore


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_max_batch_size_exceeded_in_init() -> None:
    with pytest.raises(ValueError, match="The maximum batch size supported is"):
        VoyageAIEmbeddingFunction(api_key="dummy", max_batch_size=99999999)


@pytest.mark.skipif("VOYAGE_API_KEY" not in os.environ, reason="VOYAGE_API_KEY not set, not going to test VoyageAI EF.")
def test_voyage_max_batch_size_exceeded_in_call() -> None:
    ef = VoyageAIEmbeddingFunction(api_key="dummy", max_batch_size=1)
    with pytest.raises(ValueError, match="The maximum batch size supported is"):
        ef(["test doc"] * 2)
