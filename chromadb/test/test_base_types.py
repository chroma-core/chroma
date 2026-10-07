import json

import pytest

from chromadb.base_types import SparseVector


def test_valid_sparse_vector_constructs_and_round_trips():
    sv = SparseVector(indices=[0, 2], values=[0.5, 1.0], labels=["a", "b"])
    assert sv.to_dict() == {
        "#type": "sparse_vector",
        "indices": [0, 2],
        "values": [0.5, 1.0],
        "tokens": ["a", "b"],
    }
    assert SparseVector.from_dict(sv.to_dict()) == sv


def test_valid_sparse_vector_without_labels():
    sv = SparseVector(indices=[1], values=[2.5])
    assert "tokens" not in sv.to_dict()
    assert SparseVector.from_dict(sv.to_dict()) == sv


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_rejects_non_finite_values(bad):
    with pytest.raises(ValueError, match="finite"):
        SparseVector(indices=[0], values=[bad])


@pytest.mark.parametrize("bad", [123, None, 4.5, ["x"], b"x"])
def test_rejects_non_string_labels(bad):
    with pytest.raises(ValueError, match="strings"):
        SparseVector(indices=[0], values=[1.0], labels=[bad])


def test_rejects_non_numeric_values():
    with pytest.raises(ValueError, match="numbers"):
        SparseVector(indices=[0], values=[None])  # type: ignore[list-item]


def test_from_dict_rejects_null_values():
    with pytest.raises(ValueError, match="numbers"):
        SparseVector.from_dict(
            {"#type": "sparse_vector", "indices": [0], "values": [None]}
        )


def test_json_wire_format_never_emits_null_values():
    sv = SparseVector(indices=[0, 3], values=[0.1, 0.9], labels=["tok1", "tok2"])
    payload = json.dumps(sv.to_dict())
    assert "null" not in payload
