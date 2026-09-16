import math

import pytest

from chromadb.base_types import SparseVector


def test_sparse_vector_accepts_valid_input() -> None:
    sv = SparseVector(indices=[0, 2], values=[1.0, -0.5], labels=["a", "b"])
    assert sv.to_dict() == {
        "#type": "sparse_vector",
        "indices": [0, 2],
        "values": [1.0, -0.5],
        "tokens": ["a", "b"],
    }
    assert SparseVector.from_dict(sv.to_dict()) == sv


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), -float("inf")])
def test_sparse_vector_rejects_non_finite_values(bad_value: float) -> None:
    with pytest.raises(ValueError, match="values must be finite"):
        SparseVector(indices=[0], values=[bad_value])


@pytest.mark.parametrize("bad_label", [123, 1.5, None, True])
def test_sparse_vector_rejects_non_string_labels(bad_label: object) -> None:
    with pytest.raises(ValueError, match="labels must be strings"):
        SparseVector(indices=[0], values=[1.0], labels=[bad_label])  # type: ignore[list-item]


def test_sparse_vector_rejects_non_numeric_values() -> None:
    with pytest.raises(ValueError, match="values must be numbers"):
        SparseVector(indices=[0], values=[None])  # type: ignore[list-item]


def test_sparse_vector_rejects_null_value_from_wire() -> None:
    with pytest.raises(ValueError, match="values must be numbers"):
        SparseVector.from_dict(
            {"#type": "sparse_vector", "indices": [0], "values": [None]}
        )


def test_sparse_vector_accepts_int_values() -> None:
    sv = SparseVector(indices=[0], values=[1])
    assert math.isfinite(sv.values[0])
