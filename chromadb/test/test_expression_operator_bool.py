"""Search-plan deserializers must reject bool where a number is required."""

import pytest

from chromadb.execution.expression.operator import Limit, Rank


@pytest.mark.parametrize("payload", [{"offset": True}, {"offset": False}])
def test_limit_rejects_bool_offset(payload: dict) -> None:
    with pytest.raises(TypeError, match="offset must be an integer"):
        Limit.from_dict(payload)


@pytest.mark.parametrize("payload", [{"limit": True}, {"limit": False}])
def test_limit_rejects_bool_limit(payload: dict) -> None:
    with pytest.raises(TypeError, match="limit must be an integer"):
        Limit.from_dict(payload)


def test_rank_rejects_bool_val() -> None:
    with pytest.raises(TypeError, match=r"\$val requires a number"):
        Rank.from_dict({"$val": True})


def test_rank_rejects_bool_knn_limit() -> None:
    with pytest.raises(TypeError, match="limit must be an integer"):
        Rank.from_dict({"$knn": {"query": [0.1, 0.2], "limit": True}})


@pytest.mark.parametrize("op", ["$min_k", "$max_k"])
@pytest.mark.parametrize("k", [True, False])
def test_aggregate_rejects_bool_k(op: str, k: bool) -> None:
    from chromadb.execution.expression.operator import Aggregate

    with pytest.raises(TypeError, match="must be an integer"):
        Aggregate.from_dict({op: {"keys": ["#score"], "k": k}})


def test_numeric_inputs_still_accepted() -> None:
    limit = Limit.from_dict({"offset": 2, "limit": 5})
    assert limit.offset == 2
    assert limit.limit == 5
    assert Rank.from_dict({"$val": 0.5}).value == 0.5
