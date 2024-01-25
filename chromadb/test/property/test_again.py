import uuid

import pytest
import chromadb.test.property.strategies as strategies

from unittest.mock import patch, Mock
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
from typing import Dict
from chromadb.segment import (
    VectorReader
)
from chromadb.segment import SegmentManager

from chromadb.segment.impl.manager.local import LocalSegmentManager
from chromadb.types import SegmentScope
from chromadb.db.system import SysDB
from chromadb.config import System, get_class

class QueryHistory:
    def __init__(self, n:int):
        self.n = n
        self.last_ids = []

    def update_ids(self, new_id):
        # Check if new_id is already in the list
        if new_id in self.last_ids:
            # Move the existing ID to the end of the list
            self.last_ids.remove(new_id)
            self.last_ids.append(new_id)
        else:
            # Add new_id to the list
            self.last_ids.append(new_id)
            # Keep only the last N IDs
            while len(self.last_ids) > self.n:
                self.last_ids.pop(0)
        return self.last_ids

# TODO HOW TO NOT USE A GLOBAL? CAN I SOME TEST STATE?
store: Dict[uuid.UUID, int] = {}

class SegmentManagerStateMachine(RuleBasedStateMachine):
    collections: Bundle[strategies.Collection]
    collections = Bundle("collections")
    def __init__(self, segment_manager: LocalSegmentManager, sysdb: SysDB):
        super().__init__()
        self.segment_manager = segment_manager
        self.segment_manager.start()
        self.segment_manager.reset_state()
        self.query_model = QueryHistory(n=10)
        self._sysdb = sysdb
        self.counter = 1
        self.last_query = None


    @invariant()
    def last_queried_segments_should_be_in_cache(self):
        cache_sum = 0
        index = 0
        for id in reversed(self.query_model.last_ids):
            cache_sum += store[id]
            if cache_sum >= 100 and index is not 0:
                break
            assert id in self.segment_manager._segment_cache
            index += 1

    @initialize()
    def initialize(self) -> None:
        self.segment_manager.reset_state()
        self.segment_manager.start()
        global store
        store = {}
        self.counter = 1
        self.query_model.last_ids = []

    @rule(target=collections, coll=strategies.collections())
    @precondition(lambda self: self.counter <= 50)
    def create_segment(
        self, coll: strategies.Collection
    ) -> MultipleResults[strategies.Collection]:
        segments = self.segment_manager.create_segments(coll)
        for segment in segments:
            self._sysdb.create_segment(segment)
        self.counter += 1
        store[coll["id"]] = random.randint(0, 110)
        return multiple(coll)

    @rule(coll=collections)
    def get_segment(self, coll: strategies.Collection) -> None:
        segment = self.segment_manager.get_segment(collection_id=coll["id"], type=VectorReader)
        self.query_model.update_ids(coll["id"])
        assert coll["id"] in self.segment_manager._segment_cache
        assert self.segment_manager._segment_cache[coll["id"]][SegmentScope.VECTOR]
        assert segment is not None




def mock_collection_size(self, collection_id):
    return store[collection_id]
@patch.object(LocalSegmentManager, '_get_segment_disk_size', mock_collection_size)
def test_segments(caplog: pytest.LogCaptureFixture, system: System) -> None:
    system.settings.chroma_memory_limit = 100
    if system.settings.is_persistent is False or system.settings.is_persistent is None:
        return
    run_state_machine_as_test(lambda: SegmentManagerStateMachine(segment_manager=system.require(SegmentManager), sysdb=system.require(SysDB)), _min_steps=1000)  # type: ignore


