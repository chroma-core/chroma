import uuid
from random import randint
from typing import cast, List, Any, Dict, Tuple
import hypothesis
import pytest
import hypothesis.strategies as st
from hypothesis import given, settings
import chromadb
from chromadb.api import ClientAPI
from chromadb.api.types import Embeddings, Metadatas
from chromadb.test.conftest import (
    NOT_CLUSTER_ONLY,
    override_hypothesis_profile,
)
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase
from chromadb.utils.batch_utils import create_batches
from chromadb.api.client import AdminClient
from chromadb.config import Settings


collection_st = st.shared(strategies.collections(with_hnsw_params=True), key="coll")


@given(
    collection=collection_st,
    record_set=strategies.recordsets(collection_st, min_size=1, max_size=5),
)
@settings(
    deadline=None,
    parent=override_hypothesis_profile(
        normal=hypothesis.settings(max_examples=500),
        fast=hypothesis.settings(max_examples=200),
    ),
    max_examples=2,
)
def test_add_miniscule(
    collection: strategies.Collection,
    record_set: strategies.RecordSet,
) -> None:
    _test_add(collection, record_set, True, always_compact=True)


# Hypothesis tends to generate smaller values so we explicitly segregate the
# the tests into tiers, Small, Medium. Hypothesis struggles to generate large
# record sets so we explicitly create a large record set without using Hypothesis
@given(
    collection=collection_st,
    record_set=strategies.recordsets(collection_st, min_size=1, max_size=500),
    should_compact=st.booleans(),
)
@settings(
    deadline=None,
    parent=override_hypothesis_profile(
        normal=hypothesis.settings(max_examples=500),
        fast=hypothesis.settings(max_examples=200),
    ),
)
def test_add_small(
    collection: strategies.Collection,
    record_set: strategies.RecordSet,
    should_compact: bool,
) -> None:
    _test_add(collection, record_set, should_compact)


@given(
    collection=collection_st,
    record_set=strategies.recordsets(
        collection_st,
        min_size=250,
        max_size=500,
        num_unique_metadata=5,
        min_metadata_size=1,
        max_metadata_size=5,
    ),
    should_compact=st.booleans(),
)
@settings(
    deadline=None,
    parent=override_hypothesis_profile(
        normal=hypothesis.settings(max_examples=10),
        fast=hypothesis.settings(max_examples=5),
    ),
    suppress_health_check=[
        hypothesis.HealthCheck.too_slow,
        hypothesis.HealthCheck.data_too_large,
        hypothesis.HealthCheck.large_base_example,
        hypothesis.HealthCheck.function_scoped_fixture,
    ],
)
def test_add_medium(
    collection: strategies.Collection,
    record_set: strategies.RecordSet,
    should_compact: bool,
) -> None:
    # Cluster tests transmit their results over grpc, which has a payload limit
    # This breaks the ann_accuracy invariant by default, since
    # the vector reader returns a payload of dataset size. So we need to batch
    # the queries in the ann_accuracy invariant
    _test_add(collection, record_set, should_compact, batch_ann_accuracy=True)


def _create_mcmr_clients(
    topology: str,
) -> Tuple[ClientAPI, ClientAPI]:
    """Create two clients connected to different regions for MCMR testing.

    Args:
        topology: The topology identifier for the test.

    Returns:
        A tuple of two ClientAPI instances connected to localhost:8000 and localhost:8001.
    """
    settings1 = Settings(chroma_server_host=None, chroma_server_http_port=None)
    settings2 = Settings(chroma_server_host=None, chroma_server_http_port=None)
    client1 = chromadb.HttpClient(host="localhost", port=8000, settings=settings1)
    client2 = chromadb.HttpClient(host="localhost", port=8001, settings=settings2)
    return client1, client2


def _create_isolated_database_mcmr(
    client1: ClientAPI,
    client2: ClientAPI,
    topology: str,
) -> str:
    """Create an isolated database for MCMR testing using the topology+database format.

    Args:
        client1: The first client (region 1).
        client2: The second client (region 2).
        topology: The topology identifier for the test.

    Returns:
        The database name in the format '{topology}+{database}'.
    """
    admin_settings = client1.get_settings()
    admin = AdminClient(admin_settings)
    database = f"{topology}+test_{uuid.uuid4()}"
    admin.create_database(database)
    client1.set_database(database)
    client2.set_database(database)
    return database


def _test_add(
    collection: strategies.Collection,
    record_set: strategies.RecordSet,
    should_compact: bool,
    batch_ann_accuracy: bool = False,
    always_compact: bool = False,
    topology: str = "tilt-spanning",
) -> None:
    """Test adding records to a collection across multiple regions.

    Args:
        collection: The collection configuration.
        record_set: The records to add.
        should_compact: Whether to wait for compaction.
        batch_ann_accuracy: Whether to batch the ANN accuracy checks.
        always_compact: Whether to always wait for compaction regardless of size.
        topology: Topology identifier for MCMR testing.
            Creates two clients connected to localhost:8000 and localhost:8001.
    """
    client1, client2 = _create_mcmr_clients(topology)
    _create_isolated_database_mcmr(client1, client2, topology)

    coll1 = client1.create_collection(
        name=collection.name,
        metadata=collection.metadata,  # type: ignore
        embedding_function=collection.embedding_function,
        configuration=collection.collection_config,
    )
    coll2 = client2.get_collection(
        name=collection.name,
        embedding_function=collection.embedding_function,
    )

    initial_version1 = cast(int, coll1.get_model()["version"])
    initial_version2 = cast(int, coll2.get_model()["version"])

    normalized_record_set = invariants.wrap_all(record_set)

    # TODO: The type of add() is incorrect as it does not allow for metadatas
    # like [{"a": 1}, None, {"a": 3}]
    batches = list(
        create_batches(
            api=client1,
            ids=cast(List[str], record_set["ids"]),
            embeddings=cast(Embeddings, record_set["embeddings"]),
            metadatas=cast(Metadatas, record_set["metadatas"]),
            documents=cast(List[str], record_set["documents"]),
        )
    )
    for batch_index, batch in enumerate(batches):
        if batch_index % 2 == 0:
            coll1.add(*batch)
        else:
            coll2.add(*batch)

    # Only wait for compaction if the size of the collection is
    # some minimal size
    if (
        not NOT_CLUSTER_ONLY
        and should_compact
        and (len(normalized_record_set["ids"]) > 10 or always_compact)
    ):
        # Wait for the model to be updated in each region
        wait_for_version_increase(client1, collection.name, initial_version1)
        wait_for_version_increase(client2, collection.name, initial_version2)

    # Verify invariants on both collections to ensure cross-region replication works.
    # Data is written via both coll1 and coll2, so checking both verifies that data
    # written to region 1 appears in region 2 and vice versa.
    n_results = max(1, (len(normalized_record_set["ids"]) // 10))
    for coll in (coll1, coll2):
        invariants.count(coll, cast(strategies.RecordSet, normalized_record_set))
        if batch_ann_accuracy:
            batch_size = 10
            for i in range(0, len(normalized_record_set["ids"]), batch_size):
                invariants.ann_accuracy(
                    coll,
                    cast(strategies.RecordSet, normalized_record_set),
                    n_results=n_results,
                    embedding_function=collection.embedding_function,
                    query_indices=list(
                        range(i, min(i + batch_size, len(normalized_record_set["ids"])))
                    ),
                )
        else:
            invariants.ann_accuracy(
                coll,
                cast(strategies.RecordSet, normalized_record_set),
                n_results=n_results,
                embedding_function=collection.embedding_function,
            )


# Hypothesis struggles to generate large record sets so we explicitly create
# a large record set
def create_large_recordset(
    min_size: int = 45000,
    max_size: int = 50000,
) -> strategies.RecordSet:
    size = randint(min_size, max_size)

    ids = [str(uuid.uuid4()) for _ in range(size)]
    metadatas = [{"some_key": f"{i}"} for i in range(size)]
    documents = [f"Document {i}" for i in range(size)]
    embeddings = [[1, 2, 3] for _ in range(size)]
    record_set: Dict[str, List[Any]] = {
        "ids": ids,
        "embeddings": cast(Embeddings, embeddings),
        "metadatas": metadatas,
        "documents": documents,
    }
    return cast(strategies.RecordSet, record_set)


@given(collection=collection_st, should_compact=st.booleans())
@settings(deadline=None, max_examples=5)
def test_add_large(
    collection: strategies.Collection,
    should_compact: bool,
) -> None:
    """Test adding large record sets to a collection across multiple regions.

    Args:
        collection: The collection configuration.
        should_compact: Whether to wait for compaction.
    """
    topology = "tilt-spanning"
    client1, client2 = _create_mcmr_clients(topology)
    _create_isolated_database_mcmr(client1, client2, topology)

    if (
        client1.get_settings().chroma_api_impl
        == "chromadb.api.async_fastapi.AsyncFastAPI"
    ):
        pytest.skip(
            "TODO @jai, come back and debug why CI runners fail with async + sync"
        )

    record_set = create_large_recordset(
        min_size=10000,
        max_size=50000,
    )
    coll1 = client1.create_collection(
        name=collection.name,
        metadata=collection.metadata,  # type: ignore
        embedding_function=collection.embedding_function,
    )
    coll2 = client2.get_collection(
        name=collection.name,
        embedding_function=collection.embedding_function,
    )

    normalized_record_set = invariants.wrap_all(record_set)
    initial_version1 = cast(int, coll1.get_model()["version"])
    initial_version2 = cast(int, coll2.get_model()["version"])

    batches = list(
        create_batches(
            api=client1,
            ids=cast(List[str], record_set["ids"]),
            embeddings=cast(Embeddings, record_set["embeddings"]),
            metadatas=cast(Metadatas, record_set["metadatas"]),
            documents=cast(List[str], record_set["documents"]),
        )
    )
    for batch_index, batch in enumerate(batches):
        if batch_index % 2 == 0:
            coll1.add(*batch)
        else:
            coll2.add(*batch)

    if (
        not NOT_CLUSTER_ONLY
        and should_compact
        and len(normalized_record_set["ids"]) > 10
    ):
        # Wait for the model to be updated in each region, since the record set is
        # larger, add some additional time
        wait_for_version_increase(
            client1, collection.name, initial_version1, additional_time=240
        )
        wait_for_version_increase(
            client2, collection.name, initial_version2, additional_time=240
        )

    # Verify invariants on both collections to ensure cross-region replication works.
    # Data is written via both coll1 and coll2, so checking both verifies that data
    # written to region 1 appears in region 2 and vice versa.
    n_results = max(1, (len(normalized_record_set["ids"]) // 10))
    for coll in (coll1, coll2):
        invariants.count(coll, cast(strategies.RecordSet, normalized_record_set))
        batch_size = 10
        for i in range(0, len(normalized_record_set["ids"]), batch_size):
            invariants.ann_accuracy(
                coll,
                cast(strategies.RecordSet, normalized_record_set),
                n_results=n_results,
                embedding_function=collection.embedding_function,
                query_indices=list(
                    range(i, min(i + batch_size, len(normalized_record_set["ids"])))
                ),
            )
