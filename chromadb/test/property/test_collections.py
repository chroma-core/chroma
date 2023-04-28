import pytest
import logging
import hypothesis.strategies as st
from typing import List
import chromadb
from chromadb.api import API
from chromadb.api.models.Collection import Collection
import chromadb.test.property.strategies as strategies
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    rule,
    initialize,
    multiple,
    consumes,
    run_state_machine_as_test,
)


class CollectionStateMachine(RuleBasedStateMachine):
    def __init__(self, api):
        super().__init__()
        self.existing = set()
        self.model = {}
        self.api = api

    collections = Bundle("collections")

    @initialize()
    def initialize(self):
        self.api.reset()
        self.existing = set()

    @rule(target=collections, coll=strategies.collections())
    def create_coll(self, coll):
        if coll.name in self.existing:
            with pytest.raises(Exception):
                c = self.api.create_collection(name=coll.name,
                                               metadata=coll.metadata,
                                               embedding_function=coll.embedding_function)
            return multiple()

        c = self.api.create_collection(name=coll.name,
                                       metadata=coll.metadata,
                                       embedding_function=coll.embedding_function)
        self.existing.add(coll.name)

        assert c.name == coll.name
        assert c.metadata == coll.metadata
        return coll

    @rule(coll=collections)
    def get_coll(self, coll):
        if coll.name in self.existing:
            c = self.api.get_collection(name=coll.name)
            assert c.name == coll.name
            assert c.metadata == coll.metadata
        else:
            with pytest.raises(Exception):
                self.api.get_collection(name=coll.name)

    @rule(coll=consumes(collections))
    def delete_coll(self, coll):
        if coll.name in self.existing:
            self.api.delete_collection(name=coll.name)
            self.existing.remove(coll.name)
        else:
            with pytest.raises(Exception):
                self.api.delete_collection(name=coll.name)

        with pytest.raises(Exception):
            self.api.get_collection(name=coll.name)

    @rule()
    def list_collections(self):
        colls = self.api.list_collections()
        assert len(colls) == len(self.existing)
        for c in colls:
            assert c.name in self.existing

    @rule(
        target=collections,
        coll=st.one_of(consumes(collections), strategies.collections()),
    )
    def get_or_create_coll(self, coll):
        c = self.api.get_or_create_collection(name=coll.name,
                                              metadata=coll.metadata,
                                              embedding_function=coll.embedding_function)
        assert c.name == coll.name
        if coll.metadata is not None:
            assert c.metadata == coll.metadata
        self.existing.add(coll.name)
        return coll

    @rule(
        target=collections,
        coll=consumes(collections),
        new_metadata=strategies.collection_metadata,
        new_name=st.one_of(st.none(), strategies.collection_name()),
    )
    def modify_coll(self, coll, new_metadata, new_name):
        c = self.api.get_collection(name=coll.name)

        if new_metadata is not None:
            coll.metadata = new_metadata

        if new_name is not None:
            self.existing.remove(coll.name)
            self.existing.add(new_name)
            coll.name = new_name

        c.modify(metadata=new_metadata, name=new_name)
        c = self.api.get_collection(name=coll.name)

        assert c.name == coll.name
        assert c.metadata == coll.metadata
        return coll


def test_collections(caplog, api):
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: CollectionStateMachine(api))