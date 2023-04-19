import logging
from typing import Callable
from hypothesis import given
import pytest
import chromadb
from chromadb.api import API
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.test.configurations import persist_configurations
from chromadb.test.property.test_embeddings import EmbeddingStateMachine
from hypothesis.stateful import run_state_machine_as_test, rule

CreatePersistAPI = Callable[[], API]


# TODO: fixtures should be common across tests
@pytest.fixture(scope="module", params=persist_configurations())
def create_api(request) -> CreatePersistAPI:
    configuration = request.param
    return lambda: chromadb.Client(configuration)


@given(
    collection_strategy=strategies.collections(),
    embeddings_strategy=strategies.embedding_set(),
)
def test_persist(
    create_api: CreatePersistAPI,
    collection_strategy: strategies.Collection,
    embeddings_strategy: strategies.EmbeddingSet,
):
    api_1 = create_api()
    api_1.reset()
    coll = api_1.create_collection(
        **collection_strategy, embedding_function=lambda x: None
    )
    coll.add(**embeddings_strategy)

    invariants.count(
        api_1,
        coll.name,
        len(embeddings_strategy["ids"]),
    )
    invariants.metadatas_match(coll, embeddings_strategy)
    invariants.documents_match(coll, embeddings_strategy)
    invariants.ids_match(coll, embeddings_strategy)
    invariants.ann_accuracy(coll, embeddings_strategy)

    api_1.persist()
    del api_1

    api_2 = create_api()
    coll = api_2.get_collection(
        name=collection_strategy["name"], embedding_function=lambda x: None
    )
    invariants.count(
        api_2,
        coll.name,
        len(embeddings_strategy["ids"]),
    )
    invariants.metadatas_match(coll, embeddings_strategy)
    invariants.documents_match(coll, embeddings_strategy)
    invariants.ids_match(coll, embeddings_strategy)
    invariants.ann_accuracy(coll, embeddings_strategy)


class PersistEmbeddingsStateMachine(EmbeddingStateMachine):
    def __init__(self, create_api: CreatePersistAPI):
        self.api = create_api()
        self.create_api = create_api
        super().__init__(self.api)

    @rule()
    def persist(self):
        self.api.persist()
        collection_name = self.collection.name
        del self.api
        self.api = self.create_api()
        self.collection = self.api.get_collection(collection_name)


def test_persist_embeddings_state(caplog, create_api: CreatePersistAPI):
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: PersistEmbeddingsStateMachine(create_api))


import numpy


def test_persist_state_machine_example(create_api):
    state = PersistEmbeddingsStateMachine(create_api)
    state.initialize(
        collection={"name": "A00", "metadata": None}, dtype=numpy.float16, dimension=2
    )
    # state.ann_accuracy()
    # state.count()
    # state.no_duplicates()
    state.persist()
    # state.ann_accuracy()
    # state.count()
    # state.no_duplicates()
    (v1,) = state.add_embeddings(
        embedding_set={
            "ids": ["0"],
            "embeddings": [[0.09765625, 0.430419921875]],
            "metadatas": None,
            "documents": None,
        }
    )
    # state.ann_accuracy()
    # recall: 1.0, missing 0 out of 1
    state.count()
    state.teardown()


def test_persist_state_machine_example_b(create_api):
    state = PersistEmbeddingsStateMachine(create_api)
    state.initialize(
        collection={"name": "e00", "metadata": None}, dtype=numpy.float16, dimension=2
    )
    state.ann_accuracy()
    state.count()
    state.no_duplicates()
    state.persist()
    state.ann_accuracy()
    state.count()
    state.no_duplicates()
    state.persist()
    state.ann_accuracy()
    state.count()
    state.no_duplicates()
    state.persist()
    state.ann_accuracy()
    state.count()
    state.no_duplicates()
    state.teardown()

    # state = PersistEmbeddingsStateMachine(create_api)
    # state.initialize(
    #     collection={"name": "e00", "metadata": None}, dtype=numpy.float16, dimension=2
    # )
    # state.ann_accuracy()
    # state.count()
    # state.no_duplicates()
    # state.persist()
    # state.ann_accuracy()
    # state.count()
    # state.no_duplicates()
    # state.persist()
    # state.ann_accuracy()
    # state.count()
    # state.no_duplicates()
    # state.persist()
    # state.ann_accuracy()
    # state.count()
    # state.no_duplicates()
    # state.teardown()
