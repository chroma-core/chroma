import os
import pytest
from chromadb.utils.embedding_functions.twelvelabs_embedding_function import (
    TwelveLabsEmbeddingFunction,
)

httpx = pytest.importorskip("httpx", reason="httpx not installed")

MARENGO_DIM = 512


def test_config_roundtrip() -> None:
    """get_config / build_from_config should round-trip without network."""
    ef = TwelveLabsEmbeddingFunction(
        api_key="dummy", api_key_env_var="TWELVELABS_API_KEY"
    )
    config = ef.get_config()
    assert config == {
        "api_key_env_var": "TWELVELABS_API_KEY",
        "model_name": "marengo3.0",
    }
    rebuilt = TwelveLabsEmbeddingFunction.build_from_config(config)
    assert rebuilt.get_config() == config


def test_model_name_immutable() -> None:
    ef = TwelveLabsEmbeddingFunction(api_key="dummy")
    with pytest.raises(ValueError):
        ef.validate_config_update(ef.get_config(), {"model_name": "other"})


def test_media_routing() -> None:
    """Documents are routed to text vs. image/audio URL params correctly."""
    ef = TwelveLabsEmbeddingFunction(api_key="dummy")
    assert ef._media_kind("a cat playing piano") is None
    assert ef._media_kind("https://example.com/cat.jpg") == "image"
    assert ef._media_kind("https://example.com/clip.mp3?sig=abc") == "audio"
    assert ef._media_kind("image:https://cdn.example.com/x") == "image"
    assert ef._media_kind("audio:https://cdn.example.com/y") == "audio"


def test_missing_api_key() -> None:
    saved = os.environ.pop("TWELVELABS_API_KEY", None)
    try:
        with pytest.raises(ValueError):
            TwelveLabsEmbeddingFunction(api_key_env_var="TWELVELABS_API_KEY")
    finally:
        if saved is not None:
            os.environ["TWELVELABS_API_KEY"] = saved


def test_marengo_text_embedding() -> None:
    """Live smoke test: a text document yields a 512-dim Marengo embedding."""
    if os.environ.get("TWELVELABS_API_KEY") is None:
        pytest.skip("TWELVELABS_API_KEY not set")
    ef = TwelveLabsEmbeddingFunction()
    embeddings = ef(["a cat playing piano"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == MARENGO_DIM
