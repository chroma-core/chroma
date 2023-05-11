import pytest
import logging
import hypothesis.strategies as st
import chromadb.test.property.strategies as strategies
from chromadb.api import API
import chromadb.api.types as types
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
from typing import Optional, Set


class CollectionStateMachine(RuleBasedStateMachine):  # type: ignore
    collections: Bundle
    existing: Set[str]

    collections = Bundle("collections")

    def __init__(self, api: API):
        super().__init__()
        self.existing = set()
        self.api = api

    @initialize()  # type: ignore
    def initialize(self) -> None:
        self.api.reset()
        self.existing = set()

    @rule(target=collections, coll=strategies.collections())  # type: ignore
    def create_coll(
        self, coll: strategies.Collection
    ) -> MultipleResults[strategies.Collection]:
        if coll.name in self.existing:
            with pytest.raises(Exception):
                c = self.api.create_collection(
                    name=coll.name,
                    metadata=coll.metadata,
                    embedding_function=coll.embedding_function,
                )
            return multiple()

        c = self.api.create_collection(
            name=coll.name,
            metadata=coll.metadata,
            embedding_function=coll.embedding_function,
        )
        self.existing.add(coll.name)

        assert c.name == coll.name
        assert c.metadata == coll.metadata
        return multiple(coll)

    @rule(coll=collections)  # type: ignore
    def get_coll(self, coll: strategies.Collection) -> None:
        if coll.name in self.existing:
            c = self.api.get_collection(name=coll.name)
            assert c.name == coll.name
            assert c.metadata == coll.metadata
        else:
            with pytest.raises(Exception):
                self.api.get_collection(name=coll.name)

    @rule(coll=consumes(collections))  # type: ignore
    def delete_coll(self, coll: strategies.Collection) -> None:
        if coll.name in self.existing:
            self.api.delete_collection(name=coll.name)
            self.existing.remove(coll.name)
        else:
            with pytest.raises(Exception):
                self.api.delete_collection(name=coll.name)

        with pytest.raises(Exception):
            self.api.get_collection(name=coll.name)

    @rule()  # type: ignore
    def list_collections(self) -> None:
        colls = self.api.list_collections()
        assert len(colls) == len(self.existing)
        for c in colls:
            assert c.name in self.existing

    @rule(
        target=collections,
        new_metadata=st.one_of(st.none(), strategies.collection_metadata),
        coll=st.one_of(consumes(collections), strategies.collections()),
    )  # type: ignore
    def get_or_create_coll(
        self,
        coll: strategies.Collection,
        new_metadata: Optional[types.Metadata],
    ) -> MultipleResults[strategies.Collection]:
        # In our current system, you can create with None but not update with None
        # An update with none is a no-op for the update of that field
        if coll.name not in self.existing:
            coll.metadata = new_metadata
        else:
            coll.metadata = new_metadata if new_metadata is not None else coll.metadata

        c = self.api.get_or_create_collection(
            name=coll.name,
            metadata=new_metadata,
            embedding_function=coll.embedding_function,  # type: ignore
        )
        assert c.name == coll.name
        assert c.metadata == coll.metadata
        self.existing.add(coll.name)
        return multiple(coll)

    @rule(
        target=collections,
        coll=consumes(collections),
        new_metadata=strategies.collection_metadata,
        new_name=st.one_of(st.none(), strategies.collection_name()),
    )  # type: ignore
    def modify_coll(
        self,
        coll: strategies.Collection,
        new_metadata: types.Metadata,
        new_name: Optional[str],
    ) -> MultipleResults[strategies.Collection]:
        if coll.name not in self.existing:
            with pytest.raises(Exception):
                c = self.api.get_collection(name=coll.name)
            return multiple()

        c = self.api.get_collection(name=coll.name)

        if new_metadata is not None:
            coll.metadata = new_metadata

        if new_name is not None:
            if new_name in self.existing and new_name != coll.name:
                with pytest.raises(Exception):
                    c.modify(metadata=new_metadata, name=new_name)
                return multiple()

            self.existing.remove(coll.name)
            self.existing.add(new_name)
            coll.name = new_name

        c.modify(metadata=new_metadata, name=new_name)
        c = self.api.get_collection(name=coll.name)

        assert c.name == coll.name
        assert c.metadata == coll.metadata
        return multiple(coll)


def test_collections(caplog: pytest.LogCaptureFixture, api: API) -> None:
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: CollectionStateMachine(api))
