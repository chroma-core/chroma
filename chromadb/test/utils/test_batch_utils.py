from typing import Optional

from chromadb.utils.batch_utils import create_batches


class _FakeAPI:
    """Minimal stand-in exposing only what create_batches needs."""

    def __init__(self, max_batch_size: int) -> None:
        self._max_batch_size = max_batch_size

    def get_max_batch_size(self) -> int:
        return self._max_batch_size


def _ids(n: int) -> list:
    return [str(i) for i in range(n)]


def test_input_within_limit_returns_single_batch() -> None:
    ids = _ids(3)
    batches = create_batches(_FakeAPI(10), ids)  # type: ignore[arg-type]
    assert len(batches) == 1
    # The lone batch carries the inputs through untouched.
    assert batches[0][0] is ids
    assert batches[0][1] is None
    assert batches[0][2] is None
    assert batches[0][3] is None


def test_input_at_limit_is_not_split() -> None:
    ids = _ids(4)
    batches = create_batches(_FakeAPI(4), ids)  # type: ignore[arg-type]
    assert len(batches) == 1
    assert batches[0][0] == ids


def test_input_over_limit_is_split_into_chunks() -> None:
    ids = _ids(5)
    batches = create_batches(_FakeAPI(2), ids)  # type: ignore[arg-type]
    # 5 ids with a max of 2 -> chunks of 2, 2, 1.
    assert [len(b[0]) for b in batches] == [2, 2, 1]
    # Concatenating the chunks reproduces the original order.
    flattened = [i for b in batches for i in b[0]]
    assert flattened == ids


def test_exact_multiple_splits_evenly() -> None:
    ids = _ids(6)
    batches = create_batches(_FakeAPI(3), ids)  # type: ignore[arg-type]
    assert [len(b[0]) for b in batches] == [3, 3]


def test_optional_fields_are_sliced_in_parallel() -> None:
    ids = _ids(5)
    embeddings = [[float(i)] for i in range(5)]
    metadatas: list = [{"k": i} for i in range(5)]
    documents = [f"d{i}" for i in range(5)]
    batches = create_batches(
        _FakeAPI(2),
        ids,
        embeddings,  # type: ignore[arg-type]
        metadatas,  # type: ignore[arg-type]
        documents,  # type: ignore[arg-type]
    )
    # Every field is chunked with the same boundaries as the ids.
    assert batches[0][1] == [[0.0], [1.0]]
    assert batches[0][2] == [{"k": 0}, {"k": 1}]
    assert batches[0][3] == ["d0", "d1"]
    assert batches[-1][1] == [[4.0]]
    assert batches[-1][2] == [{"k": 4}]
    assert batches[-1][3] == ["d4"]


def test_none_optional_fields_stay_none_when_split() -> None:
    ids = _ids(5)
    embeddings = [[float(i)] for i in range(5)]
    batches = create_batches(
        _FakeAPI(2),
        ids,
        embeddings,  # type: ignore[arg-type]
    )
    for _, emb, meta, docs in batches:
        assert emb is not None
        assert meta is None
        assert docs is None
    embeddings_out: Optional[list] = batches[0][1]
    assert embeddings_out == [[0.0], [1.0]]
