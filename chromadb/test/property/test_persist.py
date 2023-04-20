import logging
import multiprocessing
from typing import Callable
from hypothesis import given
import pytest
import chromadb
import traceback
from chromadb.api import API
from chromadb.config import Settings
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.test.configurations import persist_configurations
from chromadb.test.property.test_embeddings import EmbeddingStateMachine
from hypothesis.stateful import run_state_machine_as_test, rule, precondition

CreatePersistAPI = Callable[[], API]


# TODO: fixtures should be common across tests
@pytest.fixture(scope="module", params=persist_configurations())
def create_api(request) -> CreatePersistAPI:
    configuration = request.param
    return lambda: chromadb.Client(configuration)


@pytest.fixture(scope="module", params=persist_configurations())
def settings(request) -> Settings:
    configuration = request.param
    return configuration


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


def load_and_check(settings: Settings, collection_name: str, embeddings_set, conn):
    api = chromadb.Client(settings)
    coll = api.get_collection(name=collection_name, embedding_function=lambda x: None)
    try:
        invariants.count(api, coll.name, len(embeddings_set["ids"]))
        invariants.metadatas_match(coll, embeddings_set)
        invariants.documents_match(coll, embeddings_set)
        invariants.ids_match(coll, embeddings_set)
        invariants.ann_accuracy(coll, embeddings_set)
    except Exception as e:
        conn.send(e)


class PersistEmbeddingsStateMachine(EmbeddingStateMachine):
    def __init__(self, settings: Settings):
        self.api = chromadb.Client(settings)
        self.settings = settings
        super().__init__(self.api)

    @precondition(lambda self: len(self.embeddings["ids"]) >= 1)
    @rule()
    def persist(self):
        self.api.persist()
        collection_name = self.collection.name

        # Create a new process
        # And then inside the process run the invariants
        # we do this because we cannot test loading the data otherwise since
        # the data might be persist at the will of the gc
        # TODO: Once we switch off of duckdb and onto sqlite we can remove this
        conn1, conn2 = multiprocessing.Pipe()
        p = multiprocessing.Process(
            target=load_and_check,
            args=(self.settings, collection_name, self.embeddings, conn2),
        )
        p.start()
        p.join()
        if conn1.poll():
            raise conn1.recv()


def test_persist_embeddings_state(caplog, settings: Settings):
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: PersistEmbeddingsStateMachine(settings))
