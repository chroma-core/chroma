import array
from typing import Dict, Any, Callable

from chromadb.config import System, Settings
from chromadb.logservice.logservice import LogService
from chromadb.test.conftest import skip_if_not_cluster
from chromadb.test.test_api import records  # type: ignore
from chromadb.api.models.Collection import Collection

batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}

metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
}

contains_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "documents": ["this is doc1 and it's great!", "doc2 is also great!"],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}


def verify_records(
    logservice: LogService,
    collection: Collection,
    test_records_map: Dict[str, Dict[str, Any]],
    test_func: Callable,  # type: ignore
    operation: int,
) -> None:
    start_offset = 1
    for batch_records in test_records_map.values():
        test_func(**batch_records)
        pushed_records = logservice.pull_logs(collection.id, start_offset, 100)
        assert len(pushed_records) == len(batch_records["ids"])
        for i, record in enumerate(pushed_records):
            assert record.record.id == batch_records["ids"][i]
            assert record.record.operation == operation
            embedding = array.array("f", batch_records["embeddings"][i]).tobytes()
            assert record.record.vector.vector == embedding
            metadata_count = 0
            if "metadatas" in batch_records:
                metadata_count += len(batch_records["metadatas"][i])
                for key, value in batch_records["metadatas"][i].items():
                    if isinstance(value, int):
                        assert record.record.metadata.metadata[key].int_value == value
                    elif isinstance(value, float):
                        assert record.record.metadata.metadata[key].float_value == value
                    elif isinstance(value, str):
                        assert (
                            record.record.metadata.metadata[key].string_value == value
                        )
                    else:
                        assert False
            if "documents" in batch_records:
                metadata_count += 1
                assert (
                    record.record.metadata.metadata["chroma:document"].string_value
                    == batch_records["documents"][i]
                )
            assert len(record.record.metadata.metadata) == metadata_count
        start_offset += len(pushed_records)


@skip_if_not_cluster()
def test_add(api):  # type: ignore
    system = System(Settings(allow_reset=True))
    logservice = system.instance(LogService)
    system.start()
    api.reset()

    test_records_map = {
        "batch_records": batch_records,
        "metadata_records": metadata_records,
        "contains_records": contains_records,
    }

    collection = api.create_collection("testadd")
    verify_records(logservice, collection, test_records_map, collection.add, 0)


@skip_if_not_cluster()
def test_update(api):  # type: ignore
    system = System(Settings(allow_reset=True))
    logservice = system.instance(LogService)
    system.start()
    api.reset()

    test_records_map = {
        "updated_records": {
            "ids": [records["ids"][0]],
            "embeddings": [[0.1, 0.2, 0.3]],
            "metadatas": [{"foo": "bar"}],
        },
    }

    collection = api.create_collection("testupdate")
    verify_records(logservice, collection, test_records_map, collection.update, 1)


@skip_if_not_cluster()
def test_delete(api):  # type: ignore
    system = System(Settings(allow_reset=True))
    logservice = system.instance(LogService)
    system.start()
    api.reset()

    collection = api.create_collection("testdelete")

    # push 2 records
    collection.add(**contains_records)
    pushed_records = logservice.pull_logs(collection.id, 1, 100)
    assert len(pushed_records) == 2

    # delete by where does not work atm
    collection.delete(where_document={"$contains": "doc1"})
    collection.delete(where_document={"$contains": "bad"})
    collection.delete(where_document={"$contains": "great"})
    pushed_records = logservice.pull_logs(collection.id, 3, 100)
    assert len(pushed_records) == 0

    # delete by ids
    collection.delete(ids=["id1", "id2"])
    pushed_records = logservice.pull_logs(collection.id, 3, 100)
    assert len(pushed_records) == 2
    for record in pushed_records:
        assert record.record.operation == 3
        assert record.record.id in ["id1", "id2"]


@skip_if_not_cluster()
def test_upsert(api):  # type: ignore
    system = System(Settings(allow_reset=True))
    logservice = system.instance(LogService)
    system.start()
    api.reset()

    test_records_map = {
        "batch_records": batch_records,
        "metadata_records": metadata_records,
        "contains_records": contains_records,
    }

    collection = api.create_collection("testupsert")
    verify_records(logservice, collection, test_records_map, collection.upsert, 2)
