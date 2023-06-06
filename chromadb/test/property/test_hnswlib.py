import os
import shutil
import tempfile
from typing import Generator

import pytest
from chromadb.db.index.hnswlib import Hnswlib
from chromadb.config import Settings
import uuid
import numpy as np
import hypothesis.strategies as st
from hypothesis import given


@pytest.fixture(scope="module")
def settings() -> Generator[Settings, None, None]:
    save_path = tempfile.gettempdir() + "/tests/hnswlib/"
    yield Settings(persist_directory=save_path)
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


@st.composite
def elements_and_shape(
    draw: st.DrawFn, number_of_elements=st.integers(min_value=1, max_value=50000)
):
    # Generates the number of elements and
    number_of_elements = draw(number_of_elements)
    embedding_shape = draw(
        st.tuples(
            st.just(1),
            st.just(number_of_elements),
        )
    )
    return (number_of_elements, embedding_shape)


@given(values=elements_and_shape())
def test_count_tracking(
    settings: Settings,
    values: tuple[int, tuple[int, int]],
) -> None:
    number_elements, (elements, dimension) = values
    hnswlib = Hnswlib("test", settings, {}, number_elements)
    hnswlib._init_index(number_elements)
    assert hnswlib._index_metadata["curr_elements"] == 0
    assert hnswlib._index_metadata["total_elements_added"] == 0
    idA, idB = uuid.uuid4(), uuid.uuid4()

    # embeddingA = np.random.rand(1, 2)
    # Just ensure both embedding shapes are the same, no matter the size
    embeddingA = np.random.rand(elements, dimension)
    hnswlib.add([idA], embeddingA.tolist())
    assert (
        hnswlib._index_metadata["curr_elements"]
        == hnswlib._index_metadata["total_elements_added"]
        == 1
    )
    # embeddingB = np.random.rand(1, 2)
    embeddingB = np.random.rand(elements, dimension)
    hnswlib.add([idB], embeddingB.tolist())
    assert (
        hnswlib._index_metadata["curr_elements"]
        == hnswlib._index_metadata["total_elements_added"]
        == 2
    )
    hnswlib.delete_from_index(ids=[idA])
    assert hnswlib._index_metadata["curr_elements"] == 1
    assert hnswlib._index_metadata["total_elements_added"] == 2
    hnswlib.delete_from_index(ids=[idB])
    assert hnswlib._index_metadata["curr_elements"] == 0
    assert hnswlib._index_metadata["total_elements_added"] == 2


@st.composite
def dimension_params(
    draw: st.DrawFn,
    N=st.integers(min_value=1, max_value=10000),
    D=st.integers(min_value=1, max_value=100),
):
    # Force `size` value to be at most equal to N.
    N = draw(N)
    D = draw(D)
    size = draw(st.integers(min_value=1, max_value=N))
    return (N, D, size)


@given(values=dimension_params())
def test_add_delete(settings: Settings, values: tuple[int, int, int]) -> None:
    # Test adding a large number of records
    N, D, size = values
    large_records = np.random.rand(N, D).astype(np.float32).tolist()
    ids = [uuid.uuid4() for _ in range(N)]
    hnswlib = Hnswlib("test", settings, {}, N)
    hnswlib._init_index(D)
    hnswlib.add(ids, large_records)
    assert hnswlib._index_metadata["curr_elements"] == N
    assert hnswlib._index_metadata["total_elements_added"] == N

    # Test deleting a large number of records by getting a random subset of the ids
    # size = 100
    ids_to_delete = np.random.choice(np.array(ids), size=size, replace=False).tolist()
    hnswlib.delete_from_index(ids_to_delete)

    assert hnswlib._index_metadata["curr_elements"] == N - size
    assert hnswlib._index_metadata["total_elements_added"] == N
