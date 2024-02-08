import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from unittest.mock import patch
import pytest
from hypothesis import given
import hypothesis.strategies as st

from chromadb.db.impl.sqlite import TxWrapper
from chromadb.db.impl.sqlite_pool import PerThreadPool, Pool


@given(
    min_size=st.integers(min_value=1, max_value=5),
    max_size=st.integers(min_value=6, max_value=10),
    lru_check_interval=st.integers(min_value=1, max_value=10),
    connection_ttl=st.integers(min_value=1, max_value=10),
)
def test_with_max_threads(
    min_size: int,
    max_size: int,
    lru_check_interval: int,
    connection_ttl: int,
) -> None:
    def do_work(pool: Pool) -> None:
        local = threading.local()
        local.stack = []
        txw = TxWrapper(pool, local)
        with txw as cursor:
            cursor.execute("SELECT 1")
            sleep(0.1)

    warp_time: float = time.time()

    def get_warped_time() -> float:
        return warp_time

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
        with patch("time.time", side_effect=lambda: get_warped_time()) as _:
            _conn_pool = PerThreadPool(
                os.path.join(temp_dir, "chroma.sqlite3"),
                lru_check_interval=lru_check_interval,
                connection_ttl=connection_ttl,
                min_size=min_size,
                max_size=max_size,
            )
            with ThreadPoolExecutor(max_workers=max_size + 1) as executor:
                for _ in range(max_size + 1):
                    executor.submit(do_work, _conn_pool)
                executor.shutdown(wait=True)
                warp_time += warp_time + lru_check_interval + connection_ttl + 1
                _conn_pool._lru_remove_from_pool()
                assert len(_conn_pool._connections) == min_size


@given(
    min_size=st.integers(min_value=-10, max_value=0),
    max_size=st.integers(min_value=-10, max_value=0),
    lru_check_interval=st.integers(min_value=-10, max_value=0),
    connection_ttl=st.integers(min_value=-10, max_value=0),
)
def test_negative_values(
    min_size: int,
    max_size: int,
    lru_check_interval: int,
    connection_ttl: int,
) -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
        with pytest.raises(ValueError) as e:
            PerThreadPool(
                os.path.join(temp_dir, "chroma.sqlite3"),
                lru_check_interval=lru_check_interval,
                connection_ttl=connection_ttl,
                min_size=min_size,
                max_size=max_size,
            )
        assert "greater than 0" in str(e.value)
