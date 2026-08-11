from typing import Any, Dict

_UNSAFE_KWARG_KEYS = frozenset({"trust_remote_code"})

_LOCAL_MODEL_LOADER_EMBEDDING_FUNCTIONS = frozenset(
    {
        "fastembed_sparse",
        "huggingface_sparse",
        "sentence_transformer",
    }
)


def _contains_unsafe_kwarg(value: Any) -> bool:
    if isinstance(value, dict):
        for key, nested_value in value.items():
            if key in _UNSAFE_KWARG_KEYS:
                return True
            if _contains_unsafe_kwarg(nested_value):
                return True
        return False
    if isinstance(value, (list, tuple)):
        return any(_contains_unsafe_kwarg(item) for item in value)
    return False


def validate_embedding_function_kwargs_are_safe(kwargs: Any) -> None:
    if _contains_unsafe_kwarg(kwargs):
        raise ValueError(
            "trust_remote_code is not allowed as a kwarg to prevent arbitrary "
            "remote code execution"
        )


def validate_embedding_function_config_is_safe(
    name: str, config: Dict[str, Any]
) -> None:
    if name in _LOCAL_MODEL_LOADER_EMBEDDING_FUNCTIONS:
        validate_embedding_function_kwargs_are_safe(config.get("kwargs"))
