import unittest
from chromadb.db.databend import COLLECTION_TABLE_SCHEMA, EMBEDDING_TABLE_SCHEMA, db_array_schema_to_databend_schema, \
    db_schema_to_keys, Databend


class DatabendDBTest(unittest.TestCase):
    def test_array_schema_to_databend(self):
        expected_schema = "uuid String, name String, metadata String"
        assert db_array_schema_to_databend_schema(COLLECTION_TABLE_SCHEMA) == expected_schema

    def test_db_schema_to_keys(self):
        expected_keys = ['collection_uuid', 'uuid', 'embedding', 'document', 'id', 'metadata']
        assert db_schema_to_keys() == expected_keys
