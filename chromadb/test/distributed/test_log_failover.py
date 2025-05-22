# This tests a very minimal failover of a collection.  It:
# 1.  Adds half the collection to chroma.
# 2.  Initiates log failover on the collection.
# 3.  Waits for successful failover of the collection.
# 4.  Adds the other half of the collection to chroma.

import grpc
import math
import random
import time

import numpy as np

from chromadb.api import ClientAPI
from chromadb.proto.logservice_pb2 import SealLogRequest, MigrateLogRequest
from chromadb.proto.logservice_pb2_grpc import LogServiceStub
from chromadb.test.conftest import (
    reset,
    skip_if_not_cluster,
)
from chromadb.test.property import invariants

@skip_if_not_cluster()
def test_log_failover(
    client: ClientAPI,
) -> None:
    seed = time.time()
    random.seed(seed)
    print("Generating data with seed ", seed)
    reset(client)
    collection = client.create_collection(
        name="test",
        metadata={"hnsw:construction_ef": 128, "hnsw:search_ef": 128, "hnsw:M": 128},
    )

    time.sleep(1)

    print('failing over for', collection.id)
    channel = grpc.insecure_channel('localhost:50052')
    log_service_stub = LogServiceStub(channel)

    # Add 100 records, where each embedding has 3 dimensions randomly generated between 0 and 1
    ids = []
    embeddings = []
    for i in range(100):
        ids.append(str(i))
        embeddings.append(np.random.rand(1, 3)[0])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],
        )

    request = SealLogRequest(collection_id=str(collection.id))
    response = log_service_stub.SealLog(request, timeout=60)

    # Add another 100 records, where each embedding has 3 dimensions randomly generated between 0
    # and 1
    for i in range(100, 200):
        ids.append(str(i))
        embeddings.append(np.random.rand(1, 3)[0])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],
        )

    for i in range(200):
        result = collection.get(ids=[str(i)], include=["embeddings"])
        assert all([math.fabs(x - y) < 0.001 for (x, y) in zip(result["embeddings"][0], embeddings[i])])
