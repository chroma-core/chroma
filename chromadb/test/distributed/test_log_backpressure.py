# Add up to 2M records until the log-is-full message is seen.

import pytest

from chromadb.api import ClientAPI
from chromadb.test.conftest import (
    reset,
    skip_if_not_cluster,
)

EXPECTED_BACKPRESSURE_ERROR = (
    "log needs compaction before accepting more writes; "
    "please backoff exponentially and retry"
)
RECORDS = 2_000_000
BATCH_SIZE = 100
EMBEDDING = [0.0, 0.0, 0.0]


@skip_if_not_cluster()
def test_log_backpressure(
    client: ClientAPI,
) -> None:
    reset(client)
    collection = client.create_collection(
        name="test",
        metadata={"hnsw:construction_ef": 128, "hnsw:search_ef": 128, "hnsw:M": 128},
    )

    print("backpressuring for", collection.id)

    # RECORDS is intentionally high to guarantee backpressure, but the test
    # succeeds as soon as that condition is observed.
    for i in range(0, RECORDS, BATCH_SIZE):
        ids = [str(x) for x in range(i, i + BATCH_SIZE)]
        embeddings = [EMBEDDING] * BATCH_SIZE
        try:
            collection.add(ids=ids, embeddings=embeddings)
        except Exception as exc:
            print(f"Caught exception:\n{exc}")
            if EXPECTED_BACKPRESSURE_ERROR in str(exc):
                return
            raise

    pytest.fail("Expected log backpressure to be triggered.")
