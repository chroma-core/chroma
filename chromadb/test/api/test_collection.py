from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Sequence
import uuid
import numpy as np
from numpy.typing import NDArray
from chromadb.api import ClientAPI
from chromadb.api.types import URI, DataLoader, Image
from chromadb.errors import ChromaError, UniqueConstraintError
from chromadb.test.conftest import multi_region_test
from chromadb.test.ef.test_multimodal_ef import hashing_multimodal_ef


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


class _StubDataLoader(DataLoader[List[Optional[Image]]]):
    def __call__(self, uris: Sequence[Optional[URI]]) -> List[Optional[Image]]:
        def load(uri: Optional[URI]) -> Optional[NDArray[np.uint8]]:
            return None if uri is None else np.array(uri.encode())

        return [load(uri) for uri in uris]


def test_include_data_parameter_not_mutated(client: ClientAPI) -> None:
    """Regression test for issue #5857: an `include` list containing "data" must not be
    mutated in-place with an appended "uris" entry. This specifically requires "data" to
    be in `include`, since that's the only branch that appends to `request_include`; a
    collection needs a data_loader configured or "data" is rejected before that point."""
    collection = client.get_or_create_collection(
        name="test_include_data_mutation",
        data_loader=_StubDataLoader(),
        embedding_function=hashing_multimodal_ef(),
    )
    collection.add(ids=["id1", "id2"], uris=["uri_1", "uri_2"])

    include_get: List[str] = ["documents", "data"]
    collection.get(include=include_get)
    assert include_get == [
        "documents",
        "data",
    ], "get() must not mutate the include parameter"

    include_query: List[str] = ["documents", "data"]
    collection.query(
        query_uris=["uri_1"],
        n_results=1,
        include=include_query,
    )
    assert include_query == [
        "documents",
        "data",
    ], "query() must not mutate the include parameter"
