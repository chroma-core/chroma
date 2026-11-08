import pytest

from chromadb.utils.embedding_functions.fusion_embedding_function import (
    FusionEmbeddingFunction,
)


def test_name() -> None:
    assert FusionEmbeddingFunction.name() == "fusion_embedding"


def test_spaces() -> None:
    # default/supported spaces do not touch the model
    ef = object.__new__(FusionEmbeddingFunction)
    assert ef.default_space() == "cosine"
    assert set(ef.supported_spaces()) == {"cosine", "l2", "ip"}


def test_get_config_and_validate() -> None:
    ef = object.__new__(FusionEmbeddingFunction)
    ef.model_name = "EximiusLabs/fusion-embedding-2-2b-preview"
    ef.device = "cpu"
    ef.dim = 512
    config = ef.get_config()
    assert config == {
        "model_name": "EximiusLabs/fusion-embedding-2-2b-preview",
        "device": "cpu",
        "dim": 512,
    }
    FusionEmbeddingFunction.validate_config(config)


def test_validate_config_rejects_missing_required() -> None:
    with pytest.raises(Exception):
        FusionEmbeddingFunction.validate_config({"device": "cpu"})  # missing model_name


def test_encode_roundtrip() -> None:
    # Loading instantiates a ~2B model; only run where the package and weights are present.
    pytest.importorskip("fusion_embedding")
    pytest.skip("requires the fusion-embedding model weights (~2B); run manually")
