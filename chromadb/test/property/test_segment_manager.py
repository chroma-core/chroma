import uuid

import pytest
import chromadb.test.property.strategies as strategies
from unittest.mock import patch
import random
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    rule,
    initialize,
    multiple,
    precondition,
    invariant,
    run_state_machine_as_test,
    MultipleResults,
)
from typing import Dict, List
from chromadb.segment import VectorReader
from chromadb.segment import SegmentManager

from chromadb.types import SegmentScope
from chromadb.db.system import SysDB
from chromadb.config import System

# Memory limit use for testing
memory_limit = 100


# Helper class to keep tract of the last use id
class LastUse:
    n: int
    store: List[uuid.UUID]

    def __init__(self, n: int):
        self.n = n
        self.store = []

    def add(self, id: uuid.UUID) -> List[uuid.UUID]:
        if id in self.store:
            self.store.remove(id)
            self.store.append(id)
        else:
            self.store.append(id)
            while len(self.store) > self.n:
                self.store.pop(0)
        return self.store

    def reset(self) -> None:
        self.store = []


class SegmentManagerStateMachine(RuleBasedStateMachine):
    collections: Bundle[strategies.Collection]
    collections = Bundle("collections")
    collection_size_store: Dict[uuid.UUID, int] = {}
    segment_collection: Dict[uuid.UUID, uuid.UUID] = {}

    def __init__(self, system: System):
        super().__init__()
        self.segment_manager = system.require(SegmentManager)
        self.segment_manager.start()
        self.segment_manager.reset_state()
        self.last_use = LastUse(n=40)
        self.collection_created_counter = 0
        self.sysdb = system.require(SysDB)
        self.system = system

    @invariant()
    def last_queried_segments_should_be_in_cache(self) -> None:
        cache_sum = 0
        index = 0
        for id in reversed(self.last_use.store):
            cache_sum += self.collection_size_store[id]
            if cache_sum >= memory_limit and index != 0:
                break
            assert id in self.segment_manager.segment_cache[SegmentScope.VECTOR].cache  # type: ignore[attr-defined]
            index += 1

    @invariant()
    @precondition(lambda self: self.system.settings.is_persistent is True)
    def cache_should_not_be_bigger_than_settings(self) -> None:
        segment_sizes = {
            id: self.collection_size_store[id]
            for id in self.segment_manager.segment_cache[SegmentScope.VECTOR].cache  # type: ignore[attr-defined]
        }
        total_size = sum(segment_sizes.values())
        if len(segment_sizes) != 1:
            assert total_size <= memory_limit

    @initialize()
    def initialize(self) -> None:
        self.segment_manager.reset_state()
        self.segment_manager.start()
        self.collection_created_counter = 0
        self.last_use.reset()

    @rule(target=collections, coll=strategies.collections())
    @precondition(lambda self: self.collection_created_counter <= 50)
    def prepare_segments_for_new_collection(
        self, coll: strategies.Collection
    ) -> MultipleResults[strategies.Collection]:
        # TODO: Convert collection views used in tests into actual Collections / Collection models
        segments = self.segment_manager.prepare_segments_for_new_collection(coll)  # type: ignore[arg-type]
        for segment in segments:
            self.sysdb.create_segment(segment)
            self.segment_collection[segment["id"]] = coll.id
        self.collection_created_counter += 1
        self.collection_size_store[coll.id] = random.randint(0, memory_limit)
        return multiple(coll)

    @rule(coll=collections)
    def get_segment(self, coll: strategies.Collection) -> None:
        segment = self.segment_manager.get_segment(
            collection_id=coll.id, type=VectorReader
        )
        self.last_use.add(coll.id)
        assert segment is not None

    @staticmethod
    def mock_directory_size(directory: str) -> int:
        path_id = directory.split("/").pop()
        collection_id = SegmentManagerStateMachine.segment_collection[
            uuid.UUID(path_id)
        ]
        return SegmentManagerStateMachine.collection_size_store[collection_id]


@patch(
    "chromadb.segment.impl.manager.local.get_directory_size",
    SegmentManagerStateMachine.mock_directory_size,
)
def test_segment_manager(caplog: pytest.LogCaptureFixture, system: System) -> None:
    system.settings.chroma_memory_limit_bytes = memory_limit
    system.settings.chroma_segment_cache_policy = "LRU"

    run_state_machine_as_test(lambda: SegmentManagerStateMachine(system=system))  # type: ignore[no-untyped-call]
