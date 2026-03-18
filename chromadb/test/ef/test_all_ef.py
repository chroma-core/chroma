from typing import Any, cast

import pytest

from chromadb.api.types import (
    DefaultEmbeddingFunction,
    EmbeddingFunction,
    SparseEmbeddingFunction,
    validate_embedding_function,
    validate_sparse_embedding_function,
)
from chromadb.utils import embedding_functions


def _make_uninitialized_instance(ef_class: type[Any]) -> Any:
    return cast(Any, object.__new__(ef_class))


def _dense_builtin_params() -> list[Any]:
    dense_classes = set(embedding_functions.known_embedding_functions.values())
    params: list[Any] = []
    for name in sorted(embedding_functions.get_builtins()):
        ef_class = getattr(embedding_functions, name)
        if ef_class in dense_classes:
            params.append(pytest.param(ef_class, id=name))
    return params


def _sparse_builtin_params() -> list[Any]:
    sparse_classes = set(embedding_functions.sparse_known_embedding_functions.values())
    params: list[Any] = []
    for name in sorted(embedding_functions.get_builtins()):
        ef_class = getattr(embedding_functions, name)
        if ef_class in sparse_classes:
            params.append(pytest.param(ef_class, id=name))
    return params


@pytest.mark.parametrize("ef_class", _dense_builtin_params())
def test_builtin_embedding_functions_validate(ef_class: type[Any]) -> None:
    validate_embedding_function(
        cast(EmbeddingFunction[Any], _make_uninitialized_instance(ef_class))
    )


@pytest.mark.parametrize("ef_class", _sparse_builtin_params())
def test_builtin_sparse_embedding_functions_validate(ef_class: type[Any]) -> None:
    validate_sparse_embedding_function(
        cast(SparseEmbeddingFunction[Any], _make_uninitialized_instance(ef_class))
    )


def test_default_embedding_function_validates() -> None:
    validate_embedding_function(DefaultEmbeddingFunction())
