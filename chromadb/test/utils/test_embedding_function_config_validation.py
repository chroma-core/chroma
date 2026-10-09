from typing import Any, Dict, List

import pytest

from chromadb.api.collection_configuration import (
    load_collection_configuration_from_json,
    load_create_collection_configuration_from_json,
    load_update_collection_configuration_from_json,
)
from chromadb.api.types import (
    Documents,
    Embeddings,
    EmbeddingFunction,
    Schema,
    SparseEmbeddingFunction,
    SparseVector,
)
from chromadb.utils.embedding_functions import (
    known_embedding_functions,
    sparse_known_embedding_functions,
)
from chromadb.utils.embedding_functions.config_validation import (
    validate_embedding_function_config_is_safe,
    validate_embedding_function_kwargs_are_safe,
)

LOCAL_MODEL_LOADERS = [
    "sentence_transformer",
    "huggingface_sparse",
    "fastembed_sparse",
]

UNSAFE_KWARGS: List[Dict[str, Any]] = [
    {"trust_remote_code": True},
    {"trust_remote_code": False},
    {"model_kwargs": {"trust_remote_code": True}},
    {"config_kwargs": {"trust_remote_code": True}},
    {"tokenizer_kwargs": {"trust_remote_code": True}},
    {"processor_kwargs": {"trust_remote_code": True}},
    {"outer": {"inner": {"trust_remote_code": True}}},
    {"listed": [{"trust_remote_code": True}]},
    {"listed": [{"nested": ({"trust_remote_code": True},)}]},
]

BENIGN_KWARGS: List[Dict[str, Any]] = [
    {},
    {"cache_folder": "/tmp/models"},
    {"revision": "main"},
    {"token": "hf_xxx"},
    {"backend": "onnx"},
    {"model_kwargs": {"torch_dtype": "float16"}},
    {"config_kwargs": {"attn_implementation": "eager"}},
    {"model_kwargs": {"device_map": "auto"}, "revision": "refs/pr/1"},
]


@pytest.mark.parametrize("kwargs", UNSAFE_KWARGS)
def test_validate_kwargs_rejects_trust_remote_code(kwargs: Dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="trust_remote_code is not allowed"):
        validate_embedding_function_kwargs_are_safe(kwargs)


@pytest.mark.parametrize("kwargs", BENIGN_KWARGS)
def test_validate_kwargs_allows_benign_kwargs(kwargs: Dict[str, Any]) -> None:
    validate_embedding_function_kwargs_are_safe(kwargs)


def test_validate_kwargs_allows_none() -> None:
    validate_embedding_function_kwargs_are_safe(None)


@pytest.mark.parametrize("name", LOCAL_MODEL_LOADERS)
@pytest.mark.parametrize("kwargs", UNSAFE_KWARGS)
def test_validate_config_rejects_local_loader_trust_remote_code(
    name: str, kwargs: Dict[str, Any]
) -> None:
    with pytest.raises(ValueError, match="trust_remote_code is not allowed"):
        validate_embedding_function_config_is_safe(name, {"kwargs": kwargs})


@pytest.mark.parametrize("name", LOCAL_MODEL_LOADERS)
@pytest.mark.parametrize("kwargs", BENIGN_KWARGS)
def test_validate_config_allows_local_loader_benign_kwargs(
    name: str, kwargs: Dict[str, Any]
) -> None:
    validate_embedding_function_config_is_safe(name, {"kwargs": kwargs})


def test_validate_config_ignores_non_local_loaders() -> None:
    validate_embedding_function_config_is_safe(
        "openai", {"kwargs": {"trust_remote_code": True}}
    )


def test_validate_config_handles_missing_kwargs() -> None:
    validate_embedding_function_config_is_safe(
        "sentence_transformer", {"model_name": "x"}
    )


class ExplodingEmbeddingFunction(EmbeddingFunction[Documents]):
    build_calls = 0

    def __call__(self, input: Documents) -> Embeddings:
        raise AssertionError("unsafe embedding function should not be called")

    @staticmethod
    def name() -> str:
        return "sentence_transformer"

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "ExplodingEmbeddingFunction":
        ExplodingEmbeddingFunction.build_calls += 1
        raise AssertionError("unsafe embedding function should not be built")


class ExplodingSparseEmbeddingFunction(SparseEmbeddingFunction[Documents]):
    build_calls = 0

    def __call__(self, input: Documents) -> List[SparseVector]:
        raise AssertionError("unsafe sparse embedding function should not be called")

    @staticmethod
    def name() -> str:
        return "huggingface_sparse"

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "ExplodingSparseEmbeddingFunction":
        ExplodingSparseEmbeddingFunction.build_calls += 1
        raise AssertionError("unsafe sparse embedding function should not be built")


def malicious_dense_config() -> Dict[str, Any]:
    return {
        "embedding_function": {
            "name": "sentence_transformer",
            "type": "known",
            "config": {
                "model_name": "attacker/model",
                "device": "cpu",
                "normalize_embeddings": False,
                "kwargs": {"model_kwargs": {"trust_remote_code": True}},
            },
        }
    }


@pytest.mark.parametrize(
    "loader",
    [
        load_create_collection_configuration_from_json,
        load_update_collection_configuration_from_json,
        load_collection_configuration_from_json,
    ],
)
def test_loaders_reject_nested_trust_remote_code_before_build(
    monkeypatch: pytest.MonkeyPatch, loader: Any
) -> None:
    ExplodingEmbeddingFunction.build_calls = 0
    monkeypatch.setitem(
        known_embedding_functions,
        "sentence_transformer",
        ExplodingEmbeddingFunction,
    )

    with pytest.raises(ValueError, match="trust_remote_code"):
        loader(malicious_dense_config())

    assert ExplodingEmbeddingFunction.build_calls == 0


def test_schema_deserialize_drops_dense_nested_trust_remote_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ExplodingEmbeddingFunction.build_calls = 0
    monkeypatch.setitem(
        known_embedding_functions,
        "sentence_transformer",
        ExplodingEmbeddingFunction,
    )

    schema = Schema.deserialize_from_json(
        {
            "defaults": {
                "float_list": {
                    "vector_index": {
                        "enabled": True,
                        "config": {
                            "embedding_function": {
                                "name": "sentence_transformer",
                                "type": "known",
                                "config": {
                                    "model_name": "attacker/model",
                                    "device": "cpu",
                                    "normalize_embeddings": False,
                                    "kwargs": {
                                        "model_kwargs": {"trust_remote_code": True}
                                    },
                                },
                            }
                        },
                    }
                }
            },
            "keys": {},
        }
    )

    assert ExplodingEmbeddingFunction.build_calls == 0
    assert schema.defaults.float_list is not None
    assert schema.defaults.float_list.vector_index is not None
    assert schema.defaults.float_list.vector_index.config.embedding_function is None


def test_schema_deserialize_drops_sparse_nested_trust_remote_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ExplodingSparseEmbeddingFunction.build_calls = 0
    monkeypatch.setitem(
        sparse_known_embedding_functions,
        "huggingface_sparse",
        ExplodingSparseEmbeddingFunction,
    )

    schema = Schema.deserialize_from_json(
        {
            "defaults": {
                "sparse_vector": {
                    "sparse_vector_index": {
                        "enabled": True,
                        "config": {
                            "embedding_function": {
                                "name": "huggingface_sparse",
                                "type": "known",
                                "config": {
                                    "model_name": "attacker/model",
                                    "device": "cpu",
                                    "kwargs": {
                                        "processor_kwargs": {"trust_remote_code": True}
                                    },
                                },
                            }
                        },
                    }
                }
            },
            "keys": {},
        }
    )

    assert ExplodingSparseEmbeddingFunction.build_calls == 0
    assert schema.defaults.sparse_vector is not None
    assert schema.defaults.sparse_vector.sparse_vector_index is not None
    assert (
        schema.defaults.sparse_vector.sparse_vector_index.config.embedding_function
        is None
    )
