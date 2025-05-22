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
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase

RECORDS = 100

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

    # Add RECORDS records, where each embedding has 3 dimensions randomly generated between 0 and 1
    ids = []
    embeddings = []
    for i in range(RECORDS):
        ids.append(str(i))
        embeddings.append(np.random.rand(1, 3)[0])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],
        )

    request = SealLogRequest(collection_id=str(collection.id))
    response = log_service_stub.SealLog(request, timeout=60)

    # Add another RECORDS records, where each embedding has 3 dimensions randomly generated between 0
    # and 1
    for i in range(RECORDS, RECORDS + RECORDS):
        ids.append(str(i))
        embeddings.append(np.random.rand(1, 3)[0])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],
        )

    results = []
    for i in range(RECORDS + RECORDS):
        result = collection.get(ids=[str(i)], include=["embeddings"])
        if len(result["embeddings"]) == 0:
            print("missing result", i)
        results.append(result)
    for (i, result) in enumerate(results):
        if len(result["embeddings"]):
            assert all([math.fabs(x - y) < 0.001 for (x, y) in zip(result["embeddings"][0], embeddings[i])])
        else:
            assert False, "missing a result"

@skip_if_not_cluster()
def test_log_failover_with_compaction(
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

    # Add RECORDS records, where each embedding has 3 dimensions randomly generated between 0 and 1
    ids = []
    embeddings = []
    for i in range(RECORDS):
        ids.append(str(i))
        embeddings.append(np.random.rand(1, 3)[0])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],
        )
        if i == RECORDS / 2:
            # NOTE(rescrv):  This compaction tests a very particular race when migrating logs.
            # Imagine this sequence:
            # 1. Write 51 records to the go log.
            # 2. Compact all 51 records.
            # 3. Write 49 more records to the go log.
            # 4. Seal the go log.
            # 5. Log migration moves the remaining 49 records to the rust service.
            # 6. Cached frontend tries to read from a timestamp that includes all 100 records, using
            #    the initial compaction, but from a log that only has 49 records.
            # The fix is to make sure the log returns not found when a prefix of the log is
            # compacted.  This forces a fallback to repopulate the cache of the sysdb.
            wait_for_version_increase(client, collection.name, 0)

    request = SealLogRequest(collection_id=str(collection.id))
    response = log_service_stub.SealLog(request, timeout=60)

    # Add another RECORDS records, where each embedding has 3 dimensions randomly generated between 0
    # and 1
    for i in range(RECORDS, RECORDS + RECORDS):
        ids.append(str(i))
        embeddings.append(np.random.rand(1, 3)[0])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],
        )

    results = []
    for i in range(RECORDS + RECORDS):
        result = collection.get(ids=[str(i)], include=["embeddings"])
        if len(result["embeddings"]) == 0:
            print("missing result", i)
        results.append(result)
    for (i, result) in enumerate(results):
        if len(result["embeddings"]):
            assert all([math.fabs(x - y) < 0.001 for (x, y) in zip(result["embeddings"][0], embeddings[i])])
        else:
            assert False, "missing a result"
