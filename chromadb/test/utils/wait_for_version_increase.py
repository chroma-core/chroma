import time
from chromadb.api import ServerAPI
from chromadb.test.conftest import COMPACTION_SLEEP

TIMEOUT_INTERVAL = 1


def get_collection_version(api: ServerAPI, collection_name: str) -> int:
    coll = api.get_collection(collection_name)
    return coll.get_model()["version"]


def wait_for_version_increase(
    api: ServerAPI, collection_name: str, initial_version: int, additional_time: int = 0
) -> int:
    timeout = COMPACTION_SLEEP
    initial_time = time.time() + additional_time

    curr_version = get_collection_version(api, collection_name)
    while curr_version == initial_version:
        time.sleep(TIMEOUT_INTERVAL)
        if time.time() - initial_time > timeout:
            raise TimeoutError("Model was not updated in time")
        curr_version = get_collection_version(api, collection_name)

    return curr_version
