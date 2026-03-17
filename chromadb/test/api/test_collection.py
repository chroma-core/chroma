from concurrent.futures import ThreadPoolExecutor
import uuid
import chromadb
from chromadb.api import ClientAPI
from chromadb.errors import ChromaError, UniqueConstraintError
from chromadb.test.conftest import multi_region_test
from chromadb.test.data_loader.test_data_loader import (
    collection_with_data_loader,
    record_set_with_uris,
)


@multi_region_test
def test_duplicate_collection_create(
    client: ClientAPI,
) -> None:
    client.reset()

    client.create_collection(
        name="test",
        metadata={"hnsw:construction_ef": 128, "hnsw:search_ef": 128, "hnsw:M": 128},
    )

    try:
        client.create_collection(
            name="test",
            metadata={
                "hnsw:construction_ef": 128,
                "hnsw:search_ef": 128,
                "hnsw:M": 128,
            },
        )
        assert False, "Expected exception"
    except Exception as e:
        print("Collection creation failed as expected with error ", e)
        assert "already exists" in e.args[0] or isinstance(e, UniqueConstraintError)


def test_not_existing_collection_delete(
    client: ClientAPI,
) -> None:
    try:
        client.delete_collection(
            name="test101",
        )
        assert False, "Expected exception"
    except Exception as e:
        print("Collection deletion failed as expected with error ", e)
        assert "does not exist" in e.args[0]


# TODO: Spanner emulator only supports one transaction at a time
def test_multithreaded_get_or_create(client: ClientAPI) -> None:
    N_THREADS = 50
    new_name = str(uuid.uuid4())

    def create_maybe_delete_collection(i: int) -> None:
        try:
            coll = client.get_or_create_collection(new_name)
            assert coll.name == new_name
        except ChromaError as e:
            if "concurrent" not in e.message():
                raise e

        try:
            if i % 2 == 0:
                client.delete_collection(new_name)
        except ChromaError as e:
            if "does not exist" not in e.message():
                raise e

    # Stress to trigger a potential race condition
    with ThreadPoolExecutor(max_workers=N_THREADS) as executor:
        futures = [
            executor.submit(create_maybe_delete_collection, i) for i in range(N_THREADS)
        ]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                assert False, f"Thread raised an exception: {e}"
def test_include_parameter_not_mutated(
    collection_with_data_loader: chromadb.Collection, n_examples: int = 3
) -> None:
    """Regression test for issue #5857: include parameter must not be mutated in-place."""
    record_set = record_set_with_uris(n=n_examples)

    collection_with_data_loader.add(
        ids=record_set["ids"],
        uris=record_set["uris"],
    )

    # get() with "data" triggers internal append of "uris" - must not mutate caller's list
    include_get = ["data"]
    collection_with_data_loader.get(include=include_get)
    assert include_get == ["data"], "get() must not mutate include parameter"

    # query() with "data" triggers internal append of "uris" - must not mutate caller's list
    include_query = ["data"]
    collection_with_data_loader.query(
        query_uris=[record_set["uris"][0]],
        n_results=1,
        include=include_query,
    )
    assert include_query == ["data"], "query() must not mutate include parameter"

