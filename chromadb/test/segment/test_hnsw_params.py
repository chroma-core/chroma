import pytest

from chromadb.segment.impl.vector.hnsw_params import (
    HnswParams,
    PersistentHnswParams,
)


def test_bool_rejected_for_int_params():
    """bool values must not pass HNSW int validators (bool is a subclass of int)."""
    for param, value in [
        ("hnsw:construction_ef", True),
        ("hnsw:construction_ef", False),
        ("hnsw:search_ef", True),
        ("hnsw:M", True),
        ("hnsw:num_threads", False),
        ("hnsw:resize_factor", True),
    ]:
        with pytest.raises(ValueError):
            HnswParams.extract({param: value})


def test_bool_rejected_for_persistent_int_params():
    """bool values must not pass persistent HNSW int validators."""
    for param, value in [
        ("hnsw:batch_size", True),
        ("hnsw:sync_threshold", False),
    ]:
        with pytest.raises(ValueError):
            PersistentHnswParams.extract({param: value})


def test_valid_int_params_accepted():
    """Valid int values must still pass validation."""
    metadata = HnswParams.extract(
        {
            "hnsw:construction_ef": 100,
            "hnsw:search_ef": 50,
            "hnsw:M": 16,
            "hnsw:num_threads": 4,
            "hnsw:resize_factor": 1.2,
        }
    )
    assert metadata["hnsw:construction_ef"] == 100
