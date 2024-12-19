import threading
import time
from time import sleep

import numpy as np
import uuid
from typing import Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import hypothesis.strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    Bundle,
    rule,
    run_state_machine_as_test,
    consumes,
    MultipleResults,
    multiple,
)
from hypothesis import given, settings

from chromadb.segment.impl.manager.cache.cache import SegmentLRUCache, SegmentCache
from chromadb.types import Segment, SegmentScope


class LRUCacheStateMachine(RuleBasedStateMachine):
    _model: Dict[uuid.UUID, Segment]
    collection_keys = Bundle("collection_keys")

    def __init__(self, capacity: int):
        super().__init__()
        self.evicted_items = []
        self._cache = SegmentLRUCache(
            capacity=capacity,
            size_func=lambda _: 10,
            callback=lambda k, v: (self.evicted_items.append(k), self._model.pop(k)),
        )
        self._model = {}
        self._capacity = capacity

    @rule(collection_id=collection_keys)
    def test_get(self, collection_id) -> None:
        if collection_id not in self._model:
            return
        expected = self._model.get(collection_id)
        assert self._cache.get(collection_id) == expected

    @rule(collection_id=consumes(collection_keys))
    def test_pop(self, collection_id) -> None:
        if collection_id not in self._model:
            return
        expected = self._model.pop(collection_id)
        assert self._cache.pop(collection_id) == expected

    @rule(target=collection_keys)
    def test_set(self) -> MultipleResults[uuid.UUID]:
        segment = new_segment()
        collection_id = segment["collection"]
        self._model[collection_id] = segment
        self._cache.set(collection_id, segment)
        assert self._cache.get(collection_id) == segment
        assert len(self._cache.cache) <= self._capacity
        if self.evicted_items:
            last_evicted = self.evicted_items[-1]
            assert last_evicted not in self._cache.cache
        return multiple(collection_id)

    def teardown(self):
        self._cache.reset()
        self._model.clear()


@given(capacity=st.integers(min_value=10, max_value=1000))
@settings(max_examples=20)
def test_caches(capacity: int) -> None:
    run_state_machine_as_test(lambda: LRUCacheStateMachine(capacity=capacity))  # type: ignore


def new_segment(collection_id: Optional[uuid.UUID] = None) -> Segment:
    if collection_id is None:
        collection_id = uuid.uuid4()
    return Segment(
        id=uuid.uuid4(),
        type="test",
        scope=SegmentScope.VECTOR,
        collection=collection_id,
        metadata=None,
        file_paths={},
    )


class CacheSetup:
    def __init__(
        self,
        cache: SegmentCache,
        iterations: Optional[int] = 1000,
        num_threads: Optional[int] = 50,
    ):
        self.cache: SegmentCache = cache
        self.iterations = iterations
        self.num_threads = num_threads
        self.metrics: Dict[str, Any] = {
            "errors": [],
            "time_to_first_error": None,
            "error_timings": [],
        }
        self.lock = threading.Lock()


def _get_segment_disk_size(_: uuid.UUID) -> int:
    return np.random.randint(1, 10)


def callback_cache_evict(_: Segment) -> None:
    pass


@given(
    capacity=st.integers(min_value=1, max_value=1000),
    num_threads=st.integers(min_value=1, max_value=40),
    iterations=st.integers(min_value=1, max_value=800),
)
@settings(max_examples=20)
def test_thread_safety(capacity: int, num_threads: int, iterations: int) -> None:
    """Test that demonstrates thread safety issues in the LRU cache"""

    cache_setup = CacheSetup(
        SegmentLRUCache(
            capacity=capacity,
            callback=lambda k, v: callback_cache_evict(v),
            size_func=lambda k: _get_segment_disk_size(k),
        ),
        iterations=iterations,
        num_threads=num_threads,
    )

    def worker():
        """Worker that performs multiple cache operations"""
        _iterations = 0
        start_time = time.perf_counter()
        try:
            while _iterations <= cache_setup.iterations:
                _iterations += 1
                cache_keys = list(cache_setup.cache.cache.keys())
                if np.random.uniform(0, 1) < 0.5 and len(cache_keys) > 0:
                    cache_setup.cache.get(np.random.choice(cache_keys))
                else:
                    key = uuid.uuid4()
                    segment = new_segment(key)
                    cache_setup.cache.set(key, segment)
                sleep(np.random.uniform(0, 0.01))
                if np.random.uniform(0, 1) < 0.3 and len(cache_keys) > 0:
                    cache_setup.cache.get(np.random.choice(cache_keys))
                if np.random.uniform(0, 1) < 0.05:
                    cache_setup.cache.reset()
        except Exception as e:
            with cache_setup.lock:
                cache_setup.metrics["errors"].append(e)
                time_to_error = time.perf_counter() - start_time
                cache_setup.metrics["error_timings"].append(time_to_error)
                if cache_setup.metrics["time_to_first_error"] is None:
                    cache_setup.metrics["time_to_first_error"] = time_to_error

    with ThreadPoolExecutor(max_workers=cache_setup.num_threads) as executor:
        for _ in range(cache_setup.num_threads):
            executor.submit(worker)
    print(cache_setup.metrics)
    assert len(cache_setup.metrics["errors"]) == 0, "Thread safety issues found"
