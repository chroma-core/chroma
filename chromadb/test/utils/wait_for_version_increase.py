import time
from chromadb.api import ClientAPI
from chromadb.test.conftest import COMPACTION_SLEEP

TIMEOUT_INTERVAL = 1


def get_collection_version(client: ClientAPI, collection_name: str) -> int:
    coll = client.get_collection(collection_name)
    return coll.get_model()["version"]


def wait_for_version_increase(
    client: ClientAPI,
    collection_name: str,
    initial_version: int,
    additional_time: int = 0,
) -> int:
    timeout = COMPACTION_SLEEP
    deadline = time.time() + timeout + additional_time
    target_version = initial_version + 1

    curr_version = get_collection_version(client, collection_name)
    if curr_version == initial_version:
        print(
            "[wait_for_version_increase] "
            f"collection={collection_name} "
            f"waiting for version >= {target_version} "
            f"(current={curr_version}, timeout={timeout + additional_time}s)"
        )
    while curr_version == initial_version:
        time.sleep(TIMEOUT_INTERVAL)
        if time.time() > deadline:
            collection_id = client.get_collection(collection_name).id
            raise TimeoutError(
                "Model was not updated in time for "
                f"{collection_id}; waited for version >= {target_version}, "
                f"last seen version {curr_version}"
            )
        curr_version = get_collection_version(client, collection_name)

    return curr_version
