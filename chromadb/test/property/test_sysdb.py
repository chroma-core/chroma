import pytest
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    rule,
    initialize,
    multiple,
    consumes,
    run_state_machine_as_test,
    MultipleResults,
)
from typing import Dict
from uuid import uuid4

import chromadb.test.property.strategies as strategies
from chromadb.api.configuration import CollectionConfigurationInternal
from chromadb.config import System
from chromadb.db.system import SysDB
from chromadb.segment import SegmentType
from chromadb.test.conftest import NOT_CLUSTER_ONLY
from chromadb.test.db.test_system import sqlite, grpc_with_real_server
from chromadb.types import Segment, SegmentScope


class SysDBStateMachine(RuleBasedStateMachine):
    collections: Bundle[strategies.Collection] = Bundle("collections")
    created_collections: Dict[str, strategies.Collection]

    def __init__(self, sysdb: SysDB):
        super().__init__()
        self.sysdb = sysdb

    @initialize()
    def initialize(self) -> None:
        self.sysdb.reset_state()
        self.created_collections = {}

    @rule(target=collections, coll=strategies.collections())
    def create_collection(
        self, coll: strategies.Collection
    ) -> MultipleResults[strategies.Collection]:
        # TODO: Convert collection views used in tests into actual Collections / Collection models
        segments = (
            [
                Segment(
                    id=uuid4(),
                    type=SegmentType.SQLITE.value,
                    scope=SegmentScope.METADATA,
                    collection=coll.id,
                    metadata={},
                    file_paths={},
                ),
                Segment(
                    id=uuid4(),
                    type=SegmentType.HNSW_LOCAL_MEMORY.value,
                    scope=SegmentScope.VECTOR,
                    collection=coll.id,
                    metadata={},
                    file_paths={},
                ),
            ]
            if NOT_CLUSTER_ONLY
            else [
                Segment(
                    id=uuid4(),
                    type=SegmentType.BLOCKFILE_METADATA.value,
                    scope=SegmentScope.METADATA,
                    collection=coll.id,
                    metadata={},
                    file_paths={},
                ),
                Segment(
                    id=uuid4(),
                    type=SegmentType.BLOCKFILE_RECORD.value,
                    scope=SegmentScope.RECORD,
                    collection=coll.id,
                    metadata={},
                    file_paths={},
                ),
                Segment(
                    id=uuid4(),
                    type=SegmentType.HNSW_DISTRIBUTED.value,
                    scope=SegmentScope.VECTOR,
                    collection=coll.id,
                    metadata={},
                    file_paths={},
                ),
            ]
        )
        if coll.name in self.created_collections:
            with pytest.raises(Exception):
                self.sysdb.create_collection(
                    coll.id, coll.name, CollectionConfigurationInternal(), segments
                )
        else:
            self.sysdb.create_collection(
                coll.id, coll.name, CollectionConfigurationInternal(), segments
            )
            self.created_collections[coll.name] = coll
        return multiple(coll)

    @rule(coll=collections)
    def get_collection(self, coll: strategies.Collection) -> None:
        if (
            coll.name in self.created_collections
            and coll.id == self.created_collections[coll.name].id
        ):
            fetched_collections = self.sysdb.get_collections(id=coll.id)
            assert len(fetched_collections) == 1
            assert fetched_collections[0].name == coll.name
        else:
            assert len(self.sysdb.get_collections(id=coll.id)) == 0

    @rule(coll=collections)
    def get_collection_with_segments(self, coll: strategies.Collection) -> None:
        if (
            coll.name in self.created_collections
            and coll.id == self.created_collections[coll.name].id
        ):
            fetched_collection_and_segments = self.sysdb.get_collection_with_segments(
                collection_id=coll.id
            )
            assert fetched_collection_and_segments["collection"].name == coll.name
            scopes = []
            for segment in fetched_collection_and_segments["segments"]:
                assert segment["collection"] == coll.id
                scopes.append(segment["scope"])
            if NOT_CLUSTER_ONLY:
                assert len(scopes) == 2
                assert set(scopes) == {SegmentScope.METADATA, SegmentScope.VECTOR}
            else:
                assert len(scopes) == 3
                assert set(scopes) == {
                    SegmentScope.METADATA,
                    SegmentScope.RECORD,
                    SegmentScope.VECTOR,
                }
        else:
            with pytest.raises(Exception):
                self.sysdb.get_collection_with_segments(collection_id=coll.id)

    @rule(coll=consumes(collections))
    def delete_collection(self, coll: strategies.Collection) -> None:
        if (
            coll.name in self.created_collections
            and coll.id == self.created_collections[coll.name].id
        ):
            # TODO: Convert collection views used in tests into actual Collections / Collection models
            self.sysdb.delete_collection(coll.id)
            self.created_collections.pop(coll.name)
        else:
            with pytest.raises(Exception):
                self.sysdb.delete_collection(id=coll.id)


def test_sysdb(caplog: pytest.LogCaptureFixture, system: System) -> None:
    sysdb = next(sqlite()) if NOT_CLUSTER_ONLY else next(grpc_with_real_server())
    run_state_machine_as_test(lambda: SysDBStateMachine(sysdb=sysdb))  # type: ignore[no-untyped-call]
