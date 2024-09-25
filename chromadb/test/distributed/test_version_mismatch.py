import random
from typing import List, Tuple
import uuid
from chromadb.api.models.Collection import Collection
from chromadb.config import Settings, System
from chromadb.db.impl.grpc.client import GrpcSysDB
from chromadb.db.system import SysDB
from chromadb.errors import VersionMismatchError
from chromadb.segment import MetadataReader, VectorReader
from chromadb.segment.impl.metadata.grpc_segment import GrpcMetadataSegment
from chromadb.segment.impl.vector.grpc_segment import GrpcVectorSegment
from chromadb.test.conftest import reset, skip_if_not_cluster
from chromadb.api import ClientAPI
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase
from chromadb.types import RequestVersionContext, SegmentScope, VectorQuery


# Helpers
def create_test_collection(client: ClientAPI, name: str) -> Collection:
    return client.create_collection(
        name=name,
        metadata={"hnsw:construction_ef": 128, "hnsw:search_ef": 128, "hnsw:M": 128},
    )


def add_random_records_and_wait_for_compaction(
    client: ClientAPI, collection: Collection, n: int
) -> Tuple[List[str], List[List[float]], int]:
    ids = []
    embeddings = []
    for i in range(n):
        ids.append(str(i))
        embeddings.append([random.random(), random.random(), random.random()])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],  # type: ignore
        )
    final_version = wait_for_version_increase(
        client=client, collection_name=collection.name, initial_version=0
    )
    return ids, embeddings, final_version


def get_mock_frontend_system() -> System:
    settings = Settings(
        chroma_coordinator_host="localhost", chroma_server_grpc_port=50051
    )
    return System(settings)


def get_vector_segment(
    system: System, sysdb: SysDB, collection: uuid.UUID
) -> GrpcVectorSegment:
    segment = sysdb.get_segments(collection=collection, scope=SegmentScope.VECTOR)[0]
    if segment["metadata"] is None:
        segment["metadata"] = {}
    # Inject the url, replicating the behavior of the segment manager, we use the tilt grpc server url
    segment["metadata"]["grpc_url"] = "localhost:50053"  # type: ignore
    ret_segment = GrpcVectorSegment(system, segment)
    ret_segment.start()
    return ret_segment


def get_metadata_segment(
    system: System, sysdb: SysDB, collection: uuid.UUID
) -> GrpcMetadataSegment:
    segment = sysdb.get_segments(collection=collection, scope=SegmentScope.METADATA)[0]
    if segment["metadata"] is None:
        segment["metadata"] = {}
    # Inject the url, replicating the behavior of the segment manager, we use the tilt grpc server url
    segment["metadata"]["grpc_url"] = "localhost:50053"  # type: ignore
    ret_segment = GrpcMetadataSegment(system, segment)
    ret_segment.start()
    return ret_segment


def setup_vector_test(
    client: ClientAPI, n: int
) -> Tuple[VectorReader, List[str], List[List[float]], int, int]:
    reset(client)
    collection = create_test_collection(client=client, name="test_version_mismatch")
    ids, embeddings, version = add_random_records_and_wait_for_compaction(
        client=client, collection=collection, n=n
    )
    log_position = client.get_collection(collection.name)._model.log_position

    fe_system = get_mock_frontend_system()
    sysdb = GrpcSysDB(fe_system)
    sysdb.start()

    return (
        get_vector_segment(system=fe_system, sysdb=sysdb, collection=collection.id),
        ids,
        embeddings,
        version,
        log_position,
    )


def setup_metadata_test(
    client: ClientAPI, n: int
) -> Tuple[MetadataReader, List[str], List[List[float]], int, int]:
    reset(client)
    collection = create_test_collection(client=client, name="test_version_mismatch")
    ids, embeddings, version = add_random_records_and_wait_for_compaction(
        client=client, collection=collection, n=n
    )
    log_position = client.get_collection(collection.name)._model.log_position

    fe_system = get_mock_frontend_system()
    sysdb = GrpcSysDB(fe_system)
    sysdb.start()

    return (
        get_metadata_segment(system=fe_system, sysdb=sysdb, collection=collection.id),
        ids,
        embeddings,
        version,
        log_position,
    )


@skip_if_not_cluster()
def test_version_mistmatch_query_vectors(
    client: ClientAPI,
) -> None:
    N = 100
    reader, _, embeddings, compacted_version, log_position = setup_vector_test(
        client=client, n=N
    )
    request = VectorQuery(
        vectors=[embeddings[0]],
        request_version_context=RequestVersionContext(
            collection_version=compacted_version, log_position=log_position
        ),
        k=10,
        include_embeddings=False,
        allowed_ids=None,
        options=None,
    )

    reader.query_vectors(query=request)
    # Now change the collection version to > N, which should cause a version mismatch
    request["request_version_context"]["collection_version"] = N + 1
    try:
        reader.query_vectors(request)
    except VersionMismatchError:
        pass
    except Exception as e:
        assert False, f"Unexpected exception {e}"


@skip_if_not_cluster()
def test_version_mistmatch_get_vectors(
    client: ClientAPI,
) -> None:
    N = 100
    reader, _, _, compacted_version, log_position = setup_vector_test(
        client=client, n=N
    )
    request_version_context = RequestVersionContext(
        collection_version=compacted_version, log_position=log_position
    )

    reader.get_vectors(ids=None, request_version_context=request_version_context)
    # Now change the collection version to > N, which should cause a version mismatch
    request_version_context["collection_version"] = N + 1
    try:
        reader.get_vectors(request_version_context)
    except VersionMismatchError:
        pass
    except Exception as e:
        assert False, f"Unexpected exception {e}"


@skip_if_not_cluster()
def test_version_mismatch_metadata_get(
    client: ClientAPI,
) -> None:
    N = 100
    reader, _, _, compacted_version, log_position = setup_metadata_test(
        client=client, n=N
    )
    request_version_context = RequestVersionContext(
        collection_version=compacted_version, log_position=log_position
    )

    reader.get_metadata(request_version_context=request_version_context)
    # Now change the collection version to > N, which should cause a version mismatch
    request_version_context["collection_version"] = N + 1
    try:
        reader.get_metadata(request_version_context)
    except VersionMismatchError:
        pass
    except Exception as e:
        assert False, f"Unexpected exception {e}"


@skip_if_not_cluster()
def test_version_mismatch_metadata_count(
    client: ClientAPI,
) -> None:
    N = 100
    reader, _, _, compacted_version, log_position = setup_metadata_test(
        client=client, n=N
    )
    request_version_context = RequestVersionContext(
        collection_version=compacted_version, log_position=log_position
    )

    reader.count(request_version_context)
    # Now change the collection version to > N, which should cause a version mismatch
    request_version_context["collection_version"] = N + 1
    try:
        reader.count(request_version_context)
    except VersionMismatchError:
        pass
    except Exception as e:
        assert False, f"Unexpected exception {e}"
