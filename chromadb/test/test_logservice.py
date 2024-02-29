# type: ignore
from chromadb.config import System, Settings
from chromadb.db.mixins.logservice import LogService
from chromadb.test.conftest import skip_if_not_cluster

test_records_map = {}
test_records_map["batch_records"] = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}
test_records_map["metadata_records"] = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
}


@skip_if_not_cluster()
def test_add(api):
    system = System(Settings())
    logservice = system.instance(LogService)

    api.create_tenant("default_tenant")
    collection = api.create_collection("testspace")

    for batch_records in test_records_map.values():
        collection.add(**batch_records)
        pushed_records = logservice.pull_logs(collection.id, 0, 100)
        assert len(pushed_records) == len(batch_records)


# test update
# test delete
# test upsert
