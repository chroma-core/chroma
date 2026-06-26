from typing import Any, Dict


_LOCAL_MODEL_LOADERS_WITH_UNSAFE_KWARGS = {
    "fastembed_sparse",
    "huggingface_sparse",
    "sentence_transformer",
}


def validate_embedding_function_config_is_safe(
    name: str, config: Dict[str, Any]
) -> None:
    if name in _LOCAL_MODEL_LOADERS_WITH_UNSAFE_KWARGS and config.get("kwargs"):
        raise ValueError(
            f"Embedding function {name} does not allow non-empty kwargs in "
            "serialized configuration"
        )
