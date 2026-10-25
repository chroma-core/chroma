import pytest

from chromadb.utils.embedding_functions.chroma_cloud_qwen_embedding_function import (
    ChromaCloudQwenEmbeddingFunction,
    ChromaCloudQwenEmbeddingModel,
)


def test_task_none_roundtrips_through_build_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHROMA_API_KEY", "dummy")
    ef = ChromaCloudQwenEmbeddingFunction(
        model=ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
        task=None,
    )
    config = ef.get_config()
    assert config["task"] is None

    ChromaCloudQwenEmbeddingFunction.validate_config(config)
    rebuilt = ChromaCloudQwenEmbeddingFunction.build_from_config(config)

    assert rebuilt.task is None
    assert rebuilt.get_config()["task"] is None
