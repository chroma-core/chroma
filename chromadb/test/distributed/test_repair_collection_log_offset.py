# Add some records, wait for compaction, then roll back the log offset.
# Poll the log for up to 240s to see if the offset gets repaired.

import grpc
import random
import time
from typing import cast

import numpy as np

from chromadb.api import ClientAPI
from chromadb.proto.logservice_pb2 import (
    InspectLogStateRequest,
    UpdateCollectionLogOffsetRequest,
)
from chromadb.proto.logservice_pb2_grpc import LogServiceStub
from chromadb.test.conftest import (
    multi_region_test,
    reset,
    skip_if_not_cluster,
)
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase

RECORDS = 1000
BATCH_SIZE = 100
EXPECTED_REPAIRED_LOG_OFFSET = RECORDS + 1
COMPACTION_ADDITIONAL_TIME_SECONDS = 120
LOG_REPAIR_TIMEOUT_SECONDS = 240
LOG_POLL_INTERVAL_SECONDS = 1


def _inspect_collection_log_start(
    log_service_stub: LogServiceStub,
    database_name: str,
    collection_id: str,
) -> int:
    request = InspectLogStateRequest(
        database_name=database_name,
        collection_id=collection_id,
    )
    response = log_service_stub.InspectLogState(request, timeout=60)
    return int(response.start)


def _wait_for_collection_log_start(
    log_service_stub: LogServiceStub,
    database_name: str,
    collection_id: str,
    expected_start: int,
) -> None:
    deadline = time.time() + LOG_REPAIR_TIMEOUT_SECONDS
    last_start = None
    while time.time() < deadline:
        last_start = _inspect_collection_log_start(
            log_service_stub, database_name, collection_id
        )
        if last_start == expected_start:
            return
        time.sleep(LOG_POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        "Timed out waiting for collection log start "
        f"database={database_name} collection_id={collection_id} "
        f"expected={expected_start} last_seen={last_start}"
    )


@skip_if_not_cluster()
@multi_region_test
def test_repair_collection_log_offset(
    client: ClientAPI,
) -> None:
    seed = time.time()
    random.seed(seed)
    print("Generating data with seed ", seed)
    reset(client)

    channel = grpc.insecure_channel("localhost:50054")
    log_service_stub = LogServiceStub(channel)
    database_name = str(client.database)

    collection = client.create_collection(
        name="test_repair_collection_log_offset",
        metadata={"hnsw:construction_ef": 128, "hnsw:search_ef": 128, "hnsw:M": 128},
    )
    print("collection_id =", collection.id)

    initial_version = cast(int, collection.get_model()["version"])

    # Add RECORDS records, where each embedding has 3 dimensions randomly generated
    # between 0 and 1.
    for i in range(0, RECORDS, BATCH_SIZE):
        ids = []
        embeddings = []
        ids.extend([str(x) for x in range(i, i + BATCH_SIZE)])
        embeddings.extend([np.random.rand(1, 3)[0] for x in range(i, i + BATCH_SIZE)])
        collection.add(ids=ids, embeddings=embeddings)

    wait_for_version_increase(
        client,
        collection.name,
        initial_version,
        COMPACTION_ADDITIONAL_TIME_SECONDS,
    )

    collection_id = str(collection.id)
    _wait_for_collection_log_start(
        log_service_stub,
        database_name,
        collection_id,
        EXPECTED_REPAIRED_LOG_OFFSET,
    )

    request = UpdateCollectionLogOffsetRequest(
        database_name=database_name,
        collection_id=collection_id,
        log_offset=1,
    )
    log_service_stub.RollbackCollectionLogOffset(request, timeout=60)

    _wait_for_collection_log_start(
        log_service_stub,
        database_name,
        collection_id,
        EXPECTED_REPAIRED_LOG_OFFSET,
    )
