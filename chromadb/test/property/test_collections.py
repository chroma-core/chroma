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
from typing import Dict, Optional


class CollectionStateMachine(RuleBasedStateMachine):
    collections: Bundle[strategies.Collection]
    model: Dict[str, Optional[types.CollectionMetadata]]

    collections = Bundle("collections")

    def __init__(self, api: API):
        super().__init__()
        self.model = {}
        self.api = api

    @initialize()
    def initialize(self) -> None:
        self.api.reset()
        self.model = {}

    @rule(target=collections, coll=strategies.collections())
    def create_coll(
        self, coll: strategies.Collection
    ) -> MultipleResults[strategies.Collection]:
        # Metadata can either be None or a non-empty dict
        if coll.name in self.model or (
            coll.metadata is not None and len(coll.metadata) == 0
        ):
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
        self.model[coll.name] = coll.metadata

        assert c.name == coll.name
        assert c.metadata == coll.metadata
        return multiple(coll)

    @rule(coll=collections)
    def get_coll(self, coll: strategies.Collection) -> None:
        if coll.name in self.model:
            c = self.api.get_collection(name=coll.name)
            assert c.name == coll.name
            assert c.metadata == coll.metadata
        else:
            with pytest.raises(Exception):
                self.api.get_collection(name=coll.name)

    @rule(coll=consumes(collections))
    def delete_coll(self, coll: strategies.Collection) -> None:
        if coll.name in self.model:
            self.api.delete_collection(name=coll.name)
            del self.model[coll.name]
        else:
            with pytest.raises(Exception):
                self.api.delete_collection(name=coll.name)

        with pytest.raises(Exception):
            self.api.get_collection(name=coll.name)

    @rule()
    def list_collections(self) -> None:
        colls = self.api.list_collections()
        assert len(colls) == len(self.model)
        for c in colls:
            assert c.name in self.model

    @rule(
        target=collections,
        new_metadata=st.one_of(st.none(), strategies.collection_metadata),
        coll=st.one_of(consumes(collections), strategies.collections()),
    )
    def get_or_create_coll(
        self,
        coll: strategies.Collection,
        new_metadata: Optional[types.Metadata],
    ) -> MultipleResults[strategies.Collection]:
        # Cases for get_or_create

        # Case 0
        # new_metadata is none, coll is an existing collection
        # get_or_create should return the existing collection with existing metadata
        # Essentially - an update with none is a no-op

        # Case 1
        # new_metadata is none, coll is a new collection
        # get_or_create should create a new collection with the metadata of None

        # Case 2
        # new_metadata is not none, coll is an existing collection
        # get_or_create should return the existing collection with updated metadata

        # Case 3
        # new_metadata is not none, coll is a new collection
        # get_or_create should create a new collection with the new metadata, ignoring
        # the metdata of in the input coll.

        # The fact that we ignore the metadata of the generated collections is a
        # bit weird, but it is the easiest way to excercise all cases

        if new_metadata is not None and len(new_metadata) == 0:
            with pytest.raises(Exception):
                c = self.api.get_or_create_collection(
                    name=coll.name,
                    metadata=new_metadata,
                    embedding_function=coll.embedding_function,
                )
            return multiple()

        # Update model
        if coll.name not in self.model:
            # Handles case 1 and 3
            coll.metadata = new_metadata
        else:
            # Handles case 0 and 2
            coll.metadata = (
                self.model[coll.name] if new_metadata is None else new_metadata
            )
        self.model[coll.name] = coll.metadata

        # Update API
        c = self.api.get_or_create_collection(
            name=coll.name,
            metadata=new_metadata,
            embedding_function=coll.embedding_function,
        )

        # Check that model and API are in sync
        assert c.name == coll.name
        assert c.metadata == coll.metadata
        return multiple(coll)

    @rule(
        target=collections,
        coll=consumes(collections),
        new_metadata=strategies.collection_metadata,
        new_name=st.one_of(st.none(), strategies.collection_name()),
    )
    def modify_coll(
        self,
        coll: strategies.Collection,
        new_metadata: types.Metadata,
        new_name: Optional[str],
    ) -> MultipleResults[strategies.Collection]:
        if coll.name not in self.model:
            with pytest.raises(Exception):
                c = self.api.get_collection(name=coll.name)
            return multiple()

        c = self.api.get_collection(name=coll.name)

        if new_metadata is not None:
            if len(new_metadata) == 0:
                with pytest.raises(Exception):
                    c = self.api.get_or_create_collection(
                        name=coll.name,
                        metadata=new_metadata,
                        embedding_function=coll.embedding_function,
                    )
                return multiple()
            coll.metadata = new_metadata
            self.model[coll.name] = coll.metadata

        if new_name is not None:
            if new_name in self.model and new_name != coll.name:
                with pytest.raises(Exception):
                    c.modify(metadata=new_metadata, name=new_name)
                return multiple()

            del self.model[coll.name]
            self.model[new_name] = coll.metadata
            coll.name = new_name

        c.modify(metadata=new_metadata, name=new_name)
        c = self.api.get_collection(name=coll.name)

        assert c.name == coll.name
        assert c.metadata == coll.metadata
        return multiple(coll)


def test_collections(caplog: pytest.LogCaptureFixture, api: API) -> None:
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: CollectionStateMachine(api))  # type: ignore
