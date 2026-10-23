# Add up to 2M records until the log-is-full message is seen.

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from chromadb.api import ClientAPI
from chromadb.errors import RateLimitError
from chromadb.test.conftest import (
    reset,
    skip_if_not_cluster,
)

EXPECTED_BACKPRESSURE_ERROR = (
    "log needs compaction before accepting more writes; "
    "please backoff exponentially and retry"
)
EXPECTED_RATE_LIMIT_ERRORS = (
    "Too many requests; backoff and try again",
    "Rate limit exceeded.",
)
RECORDS = 2_000_000
BATCH_SIZE = 300
PARALLELISM = 4
EMBEDDING = [0.0, 0.0, 0.0]
INITIAL_RATE_LIMIT_BACKOFF_SECONDS = 0.1
MAX_RATE_LIMIT_BACKOFF_SECONDS = 1.0


def is_rate_limit_error(exc: Exception) -> bool:
    return isinstance(exc, RateLimitError) or any(
        error in str(exc) for error in EXPECTED_RATE_LIMIT_ERRORS
    )


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

    stop_event = threading.Event()

    def add_batches(worker: int) -> bool:
        # RECORDS is intentionally high to guarantee backpressure, but the test
        # succeeds as soon as that condition is observed.
        for batch in range(worker, RECORDS // BATCH_SIZE, PARALLELISM):
            if stop_event.is_set():
                return False

            i = batch * BATCH_SIZE
            ids = [str(x) for x in range(i, i + BATCH_SIZE)]
            embeddings = [EMBEDDING] * BATCH_SIZE
            backoff = INITIAL_RATE_LIMIT_BACKOFF_SECONDS
            while not stop_event.is_set():
                try:
                    collection.add(ids=ids, embeddings=embeddings)
                    break
                except Exception as exc:
                    if EXPECTED_BACKPRESSURE_ERROR in str(exc):
                        print(f"Caught exception:\n{exc}")
                        stop_event.set()
                        return True
                    if is_rate_limit_error(exc):
                        if stop_event.wait(backoff):
                            return False
                        backoff = min(
                            backoff * 2,
                            MAX_RATE_LIMIT_BACKOFF_SECONDS,
                        )
                        continue

                    print(f"Caught exception:\n{exc}")
                    stop_event.set()
                    raise
        return False

    with ThreadPoolExecutor(max_workers=PARALLELISM) as executor:
        futures = [
            executor.submit(add_batches, worker) for worker in range(PARALLELISM)
        ]
        found_backpressure = False
        for future in as_completed(futures):
            found_backpressure = future.result() or found_backpressure
            if found_backpressure:
                break

        if found_backpressure:
            for future in futures:
                future.result()
            return

    pytest.fail("Expected log backpressure to be triggered.")
