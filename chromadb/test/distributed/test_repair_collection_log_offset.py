# Add some records, wait for compaction, then roll back the log offset.
# Poll the log for up to 30s to see if the offset gets repaired.

import grpc
import random
import time
from typing import cast, List, Any, Dict

import numpy as np

from chromadb.api import ClientAPI
from chromadb.proto.logservice_pb2 import InspectLogStateRequest, UpdateCollectionLogOffsetRequest
from chromadb.proto.logservice_pb2_grpc import LogServiceStub
from chromadb.test.conftest import (
    reset,
    skip_if_not_cluster,
)
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase

RECORDS = 1000
BATCH_SIZE = 100

@skip_if_not_cluster()
def test_repair_collection_log_offset(
    client: ClientAPI,
) -> None:
    seed = time.time()
    random.seed(seed)
    print("Generating data with seed ", seed)
    reset(client)

    channel = grpc.insecure_channel('localhost:50054')
    log_service_stub = LogServiceStub(channel)

    collection = client.create_collection(
        name="test_repair_collection_log_offset",
        metadata={"hnsw:construction_ef": 128, "hnsw:search_ef": 128, "hnsw:M": 128},
    )
    print("collection_id =", collection.id)

    initial_version = cast(int, collection.get_model()["version"])

    # Add RECORDS records, where each embedding has 3 dimensions randomly generated between 0 and 1
    for i in range(0, RECORDS, BATCH_SIZE):
        ids = []
        embeddings = []
        ids.extend([str(x) for x in range(i, i + BATCH_SIZE)])
        embeddings.extend([np.random.rand(1, 3)[0] for x in range(i, i + BATCH_SIZE)])
        collection.add(ids=ids, embeddings=embeddings)

    wait_for_version_increase(client, collection.name, initial_version)

    found = False
    now = time.time()
    while time.time() - now < 240:
        request = InspectLogStateRequest(database_name=str(client.database), collection_id=str(collection.id))
        response = log_service_stub.InspectLogState(request, timeout=60)
        if '''LogPosition { offset: 1001 }''' in response.debug:
            found = True
            break
    assert found

    request = UpdateCollectionLogOffsetRequest(database_name=str(client.database), collection_id=str(collection.id), log_offset=1)
    response = log_service_stub.RollbackCollectionLogOffset(request, timeout=60)

    now = time.time()
    while time.time() - now < 240:
        request = InspectLogStateRequest(database_name=str(client.database), collection_id=str(collection.id))
        response = log_service_stub.InspectLogState(request, timeout=60)
        if '''LogPosition { offset: 1001 }''' in response.debug:
            return
        time.sleep(1)
    raise RuntimeError("Test timed out without repair")
