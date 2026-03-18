from dataclasses import dataclass
import re
from typing import Any, Callable, Optional

import numpy as np
import pytest

from chromadb.api.types import (
    SparseVector,
    validate_embedding,
    validate_embeddings,
    validate_metadata,
    validate_metadatas,
    validate_where,
)


@dataclass(frozen=True)
class AcceptedInput:
    value: Any
    description: Optional[str] = None


@dataclass(frozen=True)
class RejectableInput:
    value: Any
    description: Optional[str] = None
    match: Optional[str] = None
    error_type: type[Exception] = ValueError


@dataclass(frozen=True)
class ValidatorSpec:
    validator: Callable[[Any], Any]
    accepted_inputs: list[AcceptedInput]
    rejectable_inputs: list[RejectableInput]
    returns_input: bool = True


VALIDATORS = {
    "embedding": ValidatorSpec(
        validator=validate_embedding,
        accepted_inputs=[
            AcceptedInput(
                value=np.array([0.1, 0.2, 0.3], dtype=np.float32),
                description="single float32 embedding",
            ),
            AcceptedInput(
                value=np.array([1.0, 2.0], dtype=np.float64),
                description="single float64 embedding",
            ),
            AcceptedInput(
                value=np.array([1e308, -1e308], dtype=np.float64),
                description="single float64 embedding with extreme values",
            ),
            AcceptedInput(
                value=np.array([1, 2, 3], dtype=np.float16),
                description="single float16 embedding",
            ),
            AcceptedInput(
                value=np.array([1, 2, 3], dtype=np.int32),
                description="single int32 embedding",
            ),
            AcceptedInput(
                value=np.array([4, 5, 6], dtype=np.int64),
                description="single int64 embedding",
            ),
            AcceptedInput(
                value=np.array(
                    [np.iinfo(np.int64).max, np.iinfo(np.int64).min], dtype=np.int64
                ),
                description="single int64 embedding with extreme values",
            ),
        ],
        rejectable_inputs=[
            RejectableInput(
                value=1,
                description="top-level int",
                match="Expected embedding to be a numpy array",
            ),
            RejectableInput(
                value=[0.1, 0.2, 0.3],
                description="python list instead of ndarray",
                match="Expected embedding to be a numpy array",
            ),
            RejectableInput(
                value=np.array([[0.1, 0.2, 0.3]], dtype=np.float32),
                description="top-level 2d ndarray",
                match="Expected a 1-dimensional array",
            ),
            RejectableInput(
                value=np.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]], dtype=np.float32),
                description="2d ndarray",
                match="Expected a 1-dimensional array, got a 2-dimensional array [[0.1 0.2 0.3]\n [0.4 0.5 0.6]]",
            ),
            RejectableInput(
                value=np.array(1.0, dtype=np.float32),
                description="0-dimensional ndarray",
                match="Expected a 1-dimensional array",
            ),
            RejectableInput(
                value=np.array([], dtype=np.float32),
                description="empty 1-dimensional ndarray",
                match="Expected embedding to be a 1-dimensional numpy array with at least 1 int/float value",
            ),
            RejectableInput(
                value=np.array([True, False]),
                description="bool dtype",
                match="Expected each value in the embedding to be a int or float",
            ),
            RejectableInput(
                value=np.array(["a", "b"], dtype=object),
                description="object dtype",
                match="Expected each value in the embedding to be a int or float",
            ),
        ],
    ),
    "embeddings": ValidatorSpec(
        validator=validate_embeddings,
        accepted_inputs=[
            AcceptedInput(
                value=[np.array([0.1, 0.2, 0.3], dtype=np.float32)],
                description="single float32 embedding list",
            ),
            AcceptedInput(
                value=[
                    np.array([1.0, 2.0], dtype=np.float64),
                    np.array([3.0, 4.0], dtype=np.float16),
                ],
                description="multiple float embeddings with supported dtypes",
            ),
            AcceptedInput(
                value=[
                    np.array([1, 2, 3], dtype=np.int32),
                    np.array([4, 5, 6], dtype=np.int64),
                ],
                description="multiple integer embeddings with supported dtypes",
            ),
        ],
        rejectable_inputs=[
            RejectableInput(
                value=1,
                description="top-level int",
                match="Expected embeddings to be a list",
            ),
            RejectableInput(
                value=np.array([[0.1, 0.2, 0.3]], dtype=np.float32),
                description="top-level 2d ndarray",
                match="Expected embeddings to be a list",
            ),
            RejectableInput(
                value=np.array([0.1, 0.2, 0.3], dtype=np.float32),
                description="top-level 1d ndarray",
                match="Expected embeddings to be a list",
            ),
            RejectableInput(
                value=[],
                description="empty list",
                match="Expected embeddings to be a list with at least one item",
            ),
            RejectableInput(
                value=[[0.1, 0.2, 0.3]],
                description="list of python lists",
                match="Expected embedding to be a numpy array",
            ),
            RejectableInput(
                value=[np.array(1.0, dtype=np.float32)],
                description="list containing 0-dimensional ndarray",
                match="Expected a 1-dimensional array",
            ),
            RejectableInput(
                value=[np.array([], dtype=np.float32)],
                description="list containing empty 1-dimensional ndarray",
                match="Expected embedding to be a 1-dimensional numpy array with at least 1 int/float value",
            ),
            RejectableInput(
                value=[np.array([True, False])],
                description="list containing bool dtype",
                match="Expected each value in the embedding to be a int or float",
            ),
            RejectableInput(
                value=[np.array(["a", "b"], dtype=object)],
                description="list containing object dtype",
                match="Expected each value in the embedding to be a int or float",
            ),
        ],
    ),
    "metadata": ValidatorSpec(
        validator=validate_metadata,
        accepted_inputs=[
            AcceptedInput(value=None, description="none metadata"),
            AcceptedInput(
                value={"name": "chroma", "count": 3, "score": 1.5, "active": True},
                description="primitive metadata values",
            ),
            AcceptedInput(
                value={"optional": None, "tags": ["a", "b"], "scores": [1, 2, 3]},
                description="metadata with none and homogeneous lists",
            ),
            AcceptedInput(
                value={
                    "sparse": SparseVector(indices=[1, 3], values=[0.5, 1.5]),
                    "flags": [True, False, True],
                },
                description="metadata with sparse vector",
            ),
        ],
        rejectable_inputs=[
            RejectableInput(
                value=1,
                description="top-level int",
                match="Expected metadata to be a dict or None",
            ),
            RejectableInput(
                value={},
                description="empty metadata dict",
                match="Expected metadata to be a non-empty dict",
            ),
            RejectableInput(
                value={"chroma:document": "reserved"},
                description="reserved key",
                match="Expected metadata to not contain the reserved key",
            ),
            RejectableInput(
                value={1: "value"},
                description="non-string key",
                match="Expected metadata key to be a str",
                error_type=TypeError,
            ),
            RejectableInput(
                value={"nested": {"a": 1}},
                description="nested dict value",
                match="Expected metadata value to be a str, int, float, bool, SparseVector, list, or None",
            ),
            RejectableInput(
                value={"tags": []},
                description="empty metadata list value",
                match="Expected metadata list value for key 'tags' to be non-empty",
            ),
            RejectableInput(
                value={"tags": ["a", 1]},
                description="mixed-type metadata list value",
                match="Expected metadata list value for key 'tags' to contain only str, int, float, or bool",
            ),
            RejectableInput(
                value={"tags": [["nested"]]},
                description="nested list metadata value",
                match="Expected metadata list value for key 'tags' to contain only str, int, float, or bool",
            ),
        ],
    ),
    "metadatas": ValidatorSpec(
        validator=validate_metadatas,
        accepted_inputs=[
            AcceptedInput(value=[], description="empty metadata list"),
            AcceptedInput(
                value=[None, {"name": "chroma"}, {"count": 1, "flags": [True, False]}],
                description="metadata list with none and valid dicts",
            ),
            AcceptedInput(
                value=[
                    {"sparse": SparseVector(indices=[0], values=[1.0])},
                    {"tags": ["a", "b"]},
                ],
                description="metadata list with sparse vector",
            ),
        ],
        rejectable_inputs=[
            RejectableInput(
                value=r"{key: 1, key2: 2}",
                description="top-level string",
                match="Expected metadatas to be a list",
            ),
            RejectableInput(
                value={"name": "chroma"},
                description="top-level dict",
                match="Expected metadatas to be a list",
            ),
            RejectableInput(
                value=[{}],
                description="contains empty metadata dict",
                match="Expected metadata to be a non-empty dict",
            ),
            RejectableInput(
                value=[{"tags": ["a", 1]}],
                description="contains invalid metadata list value",
                match="Expected metadata list value for key 'tags' to contain only str, int, float, or bool",
            ),
            RejectableInput(
                value=[{1: "value"}],
                description="contains non-string key",
                match="Expected metadata key to be a str",
                error_type=TypeError,
            ),
        ],
    ),
    "where": ValidatorSpec(
        validator=validate_where,
        accepted_inputs=[
            AcceptedInput(
                value={"name": "chroma"},
                description="field equality with string",
            ),
            AcceptedInput(
                value={"count": {"$gte": 3}},
                description="numeric comparison operator",
            ),
            AcceptedInput(
                value={"status": {"$in": ["new", "processing"]}},
                description="in operator with homogeneous string list",
            ),
            AcceptedInput(
                value={"tags": {"$contains": "prod"}},
                description="contains operator on metadata field",
            ),
            AcceptedInput(
                value={"#document": {"$contains": "needle"}},
                description="contains operator on document field",
            ),
            AcceptedInput(
                value={
                    "$and": [
                        {"tenant": "default"},
                        {"score": {"$lt": 0.5}},
                    ]
                },
                description="and operator with nested expressions",
            ),
            AcceptedInput(
                value={
                    "$or": [
                        {"name": "alpha"},
                        {"priority": {"$nin": [1, 2]}},
                    ]
                },
                description="or operator with nested expressions",
            ),
        ],
        rejectable_inputs=[
            RejectableInput(
                value=["not", "a", "dict"],
                description="top-level list",
                match="Expected where to be a dict",
            ),
            RejectableInput(
                value={},
                description="empty where dict",
                match="Expected where to have exactly one operator",
            ),
            RejectableInput(
                value={"name": "chroma", "count": 3},
                description="multiple top-level keys",
                match="Expected where to have exactly one operator",
            ),
            RejectableInput(
                value={1: "value"},
                description="non-string top-level key",
                match="Expected where key to be a str",
            ),
            RejectableInput(
                value={"$contains": "prod"},
                description="contains as top-level key",
                match="Expected where key to be a metadata field name or a logical operator",
            ),
            RejectableInput(
                value={"tags": ["prod", "staging"]},
                description="top-level list value for field",
                match="Expected where value to be a str, int, float, or operator expression",
            ),
            RejectableInput(
                value={"$and": "not-a-list"},
                description="and with non-list operand",
                match="Expected where value for $and or $or to be a list of where expressions",
            ),
            RejectableInput(
                value={"$or": [{"name": "alpha"}]},
                description="or with too few expressions",
                match="Expected where value for $and or $or to be a list with at least two where expressions",
            ),
            RejectableInput(
                value={"count": {"$gt": "3"}},
                description="numeric comparison with string operand",
                match="Expected operand value to be an int or a float for operator $gt",
            ),
            RejectableInput(
                value={"status": {"$in": "new"}},
                description="in with non-list operand",
                match="Expected operand value to be an list for operator $in",
            ),
            RejectableInput(
                value={"status": {"$in": []}},
                description="in with empty list operand",
                match="Expected where operand value to be a non-empty list, and all values to be of the same type",
            ),
            RejectableInput(
                value={"status": {"$in": ["new", 1]}},
                description="in with mixed-type list operand",
                match="Expected where operand value to be a non-empty list, and all values to be of the same type",
            ),
            RejectableInput(
                value={"tags": {"$contains": ["prod"]}},
                description="contains with list operand",
                match="Expected operand value to be a str, int, float, or bool for operator $contains",
            ),
            RejectableInput(
                value={"#document": {"$contains": 1}},
                description="document contains with non-string operand",
                match="Expected operand value to be a str for operator $contains on #document",
            ),
            RejectableInput(
                value={"count": {"$between": [1, 2]}},
                description="unknown operator",
                match="Expected where operator to be one of $gt, $gte, $lt, $lte, $ne, $eq, $in, $nin, $contains, $not_contains",
            ),
            RejectableInput(
                value={"count": {"$gt": 1, "$lt": 5}},
                description="operator expression with multiple operators",
                match="Expected operator expression to have exactly one operator",
            ),
        ],
        returns_input=False,
    ),
}


def _case_id(case: AcceptedInput | RejectableInput) -> str:
    return case.description or repr(case.value)


def _assert_validator_accepts_input(
    validator: Callable[[Any], Any], case: AcceptedInput, returns_input: bool
) -> None:
    value = case.value
    validated = validator(value)
    if returns_input:
        assert validated is value
    else:
        assert validated is None


def _assert_validator_rejects_input(
    validator: Callable[[Any], Any], case: RejectableInput
) -> None:
    with pytest.raises(
        case.error_type, match=re.escape(case.match) if case.match else None
    ):
        validator(case.value)


def _accepted_params() -> list[Any]:
    params: list[Any] = []
    for validator_name, spec in VALIDATORS.items():
        for case in spec.accepted_inputs:
            params.append(
                pytest.param(
                    spec.validator,
                    spec.returns_input,
                    case,
                    id=f"{validator_name}:{_case_id(case)}",
                )
            )
    return params


def _rejectable_params() -> list[Any]:
    params: list[Any] = []
    for validator_name, spec in VALIDATORS.items():
        for case in spec.rejectable_inputs:
            params.append(
                pytest.param(
                    spec.validator,
                    case,
                    id=f"{validator_name}:{_case_id(case)}",
                )
            )
    return params


@pytest.mark.parametrize(
    ("validator", "returns_input", "case"),
    _accepted_params(),
)
def test_validators_accept_valid_inputs(
    validator: Callable[[Any], Any],
    returns_input: bool,
    case: AcceptedInput,
) -> None:
    _assert_validator_accepts_input(validator, case, returns_input)


@pytest.mark.parametrize(
    ("validator", "case"),
    _rejectable_params(),
)
def test_validators_reject_invalid_inputs(
    validator: Callable[[Any], Any],
    case: RejectableInput,
) -> None:
    _assert_validator_rejects_input(validator, case)
