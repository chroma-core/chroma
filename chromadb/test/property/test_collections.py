import pytest
import logging
import hypothesis.strategies as st
import chromadb.test.property.strategies as strategies
from chromadb.api import ClientAPI
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

import numpy
import uuid
from chromadb.test.property.strategies import hashing_embedding_function


class CollectionStateMachine(RuleBasedStateMachine):
    collections: Bundle[strategies.ExternalCollection]
    _model: Dict[str, Optional[types.CollectionMetadata]]

    collections = Bundle("collections")

    def __init__(self, api: ClientAPI):
        super().__init__()
        self._model = {}
        self.api = api

    @initialize()
    def initialize(self) -> None:
        self.api.reset()
        self._model = {}

    @rule(target=collections, coll=strategies.collections())
    def create_coll(
        self, coll: strategies.ExternalCollection
    ) -> MultipleResults[strategies.ExternalCollection]:
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
        self.set_model(coll.name, coll.metadata)

        assert c.name == coll.name
        assert c.metadata == self.model[coll.name]
        return multiple(coll)

    @rule(coll=collections)
    def get_coll(self, coll: strategies.ExternalCollection) -> None:
        if coll.name in self.model:
            c = self.api.get_collection(name=coll.name)
            assert c.name == coll.name
            assert c.metadata == self.model[coll.name]
        else:
            with pytest.raises(Exception):
                self.api.get_collection(name=coll.name)

    @rule(coll=consumes(collections))
    def delete_coll(self, coll: strategies.ExternalCollection) -> None:
        if coll.name in self.model:
            self.api.delete_collection(name=coll.name)
            self.delete_from_model(coll.name)
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

    # @rule for list_collections with limit and offset
    @rule(
        limit=st.integers(min_value=1, max_value=5),
        offset=st.integers(min_value=0, max_value=5),
    )
    def list_collections_with_limit_offset(self, limit: int, offset: int) -> None:
        colls = self.api.list_collections(limit=limit, offset=offset)
        total_collections = self.api.count_collections()

        # get all collections
        all_colls = self.api.list_collections()
        # manually slice the collections based on the given limit and offset
        man_colls = all_colls[offset : offset + limit]

        # given limit and offset, make various assertions regarding the total number of collections
        if limit + offset > total_collections:
            assert len(colls) == max(total_collections - offset, 0)
            # assert that our manually sliced collections are the same as the ones returned by the API
            assert colls == man_colls

        else:
            assert len(colls) == limit

    @rule(
        target=collections,
        new_metadata=st.one_of(st.none(), strategies.collection_metadata),
        coll=st.one_of(consumes(collections), strategies.collections()),
    )
    def get_or_create_coll(
        self,
        coll: strategies.ExternalCollection,
        new_metadata: Optional[types.Metadata],
    ) -> MultipleResults[strategies.ExternalCollection]:
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
        self.set_model(coll.name, coll.metadata)

        # Update API
        c = self.api.get_or_create_collection(
            name=coll.name,
            metadata=new_metadata,
            embedding_function=coll.embedding_function,
        )

        # Check that model and API are in sync
        assert c.name == coll.name
        assert c.metadata == self.model[coll.name]
        return multiple(coll)

    @rule(
        target=collections,
        coll=consumes(collections),
        new_metadata=strategies.collection_metadata,
        new_name=st.one_of(st.none(), strategies.collection_name()),
    )
    def modify_coll(
        self,
        coll: strategies.ExternalCollection,
        new_metadata: types.Metadata,
        new_name: Optional[str],
    ) -> MultipleResults[strategies.ExternalCollection]:
        if coll.name not in self.model:
            with pytest.raises(Exception):
                c = self.api.get_collection(name=coll.name)
            return multiple()

        c = self.api.get_collection(name=coll.name)
        _metadata: Optional[Mapping[str, Any]] = self.model[coll.name]
        _name: str = coll.name
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
            _metadata = new_metadata

        if new_name is not None:
            if new_name in self.model and new_name != coll.name:
                with pytest.raises(Exception):
                    c.modify(metadata=new_metadata, name=new_name)
                return multiple()

            self.delete_from_model(coll.name)
            coll.name = new_name
            _name = new_name

        self.set_model(_name, _metadata)
        c.modify(metadata=_metadata, name=_name)
        c = self.api.get_collection(name=coll.name)

        assert c.name == coll.name
        assert c.metadata == self.model[coll.name]
        return multiple(coll)

    def set_model(
        self,
        name: str,
        metadata: Optional[types.CollectionMetadata],
    ) -> None:
        model = self.model
        model[name] = metadata

    def delete_from_model(self, name: str) -> None:
        model = self.model
        del model[name]

    @property
    def model(self) -> Dict[str, Optional[types.CollectionMetadata]]:
        return self._model


def test_collections(caplog: pytest.LogCaptureFixture, api: ClientAPI) -> None:
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: CollectionStateMachine(api))  # type: ignore


# Below are tests that have failed in the past. If your test fails, please add
# it to protect against regressions in the test harness itself. If you need
# help doing so, talk to ben.


def test_previously_failing_one(api: ClientAPI) -> None:
    state = CollectionStateMachine(api)
    state.initialize()
    # I don't know why the typechecker is red here. This code is correct and is
    # pulled from the logs.
    (v1,) = state.get_or_create_coll(
        coll=strategies.ExternalCollection(
            name='jjn2yjLW1zp2T\n',
            metadata=None,
            embedding_function=hashing_embedding_function(dtype=numpy.float32, dim=863)
        ),
        new_metadata=None
    )
    (v6,) = state.get_or_create_coll(
        coll=strategies.ExternalCollection(
            name='jjn2yjLW1zp2T\n',
            metadata=None,
            embedding_function=hashing_embedding_function(dtype=numpy.float32, dim=863)
        ),
        new_metadata=None
    )
    state.modify_coll(
        coll=v1,
        new_metadata={'7': -1281, 'fGe': -0.0, 'K5j': 'im'},
        new_name=None
    )
    state.modify_coll(
        coll=v6,
        new_metadata=None,
        new_name=None
    )


# https://github.com/chroma-core/chroma/commit/cf476d70f0cebb7c87cb30c7172ba74d6ea175cd#diff-e81868b665d149bb315d86890dea6fc6a9fc9fc9ea3089aa7728142b54f622c5R210
def test_previously_failing_two(api: ClientAPI) -> None:
    state = CollectionStateMachine(api)
    state.initialize()
    (v13,) = state.get_or_create_coll(
        coll=strategies.ExternalCollection(
            name='C1030',
            metadata={},
            embedding_function=hashing_embedding_function(dim=2, dtype=numpy.float32)
        ),
        new_metadata=None
    )
    (v15,) = state.modify_coll(
        coll=v13,
        new_metadata={'0': '10', '40': '0', 'p1nviWeL7fO': 'qN', '7b': 'YS', 'VYWq4LEMWjCo': True},
        new_name='OF5F0MzbQg\n'
    )
    state.get_or_create_coll(
        coll=strategies.ExternalCollection(
            name='VS0QGh',
            metadata={'h': 5.681951615025145e-227, 'A1': 61126, 'uhUhLEEMfeC_kN': 2147483647, 'weF': 'pSP', 'B3DSaP': False, '6H533K': 1.192092896e-07},
            embedding_function=hashing_embedding_function(dim=1915, dtype=numpy.float32)
        ),
        new_metadata={'xVW09xUpDZA': 31734,
                      'g': 1.1,
                      'n1dUTalF-MY': -1000000.0,
                      'y': 'G3EtXTZ',
                      'ugXZ_hK': 5494
                      }
    )
    (v17,) = state.modify_coll(
        coll=v15,
        new_metadata={'L35J2S': 'K0l026'},
        new_name='Ai1\n'
    )
    (v18,) = state.get_or_create_coll(coll=v13, new_metadata=None)
    state.get_or_create_coll(
        coll=strategies.ExternalCollection(
            name='VS0QGh',
            metadata=None,
            embedding_function=hashing_embedding_function(dim=326, dtype=numpy.float16)
        ),
        new_metadata=None
    )