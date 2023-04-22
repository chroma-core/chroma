from typing import Callable
from hypothesis import given
import hypothesis.strategies as st
import pytest
import chromadb
from chromadb.api import API
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.test.configurations import persist_configurations


CreatePersistAPI = Callable[[], API]


# TODO: fixtures should be common across tests
@pytest.fixture(scope="module", params=persist_configurations())
def create_api(request) -> CreatePersistAPI:
    configuration = request.param
    return lambda: chromadb.Client(configuration)


collection_st = st.shared(strategies.collections(), key="coll")
@given(
    collection_strategy=collection_st,
    embeddings_strategy=strategies.recordsets(collection_st),
)
def test_persist(
    create_api: CreatePersistAPI,
    collection_strategy: strategies.Collection,
    embeddings_strategy: strategies.RecordSet,
):
    api_1 = create_api()
    api_1.reset()
    coll = api_1.create_collection(name=collection_strategy.name,
                                   metadata=collection_strategy.metadata,
                                   embedding_function=lambda x: None)

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
        name=collection_strategy.name, embedding_function=lambda x: None
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
