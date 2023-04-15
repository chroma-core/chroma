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

    def test_create_collection_table(self):
        import chromadb
        from chromadb.config import Settings
        # need databend query server
        api = chromadb.Client(Settings(chroma_db_impl="databend", databend_host="127.0.0.1", databend_port="8000",
                                       databend_user="root", databend_password="root", databend_database="default",
                                       persist_directory="./"))
        print(api.get_version())
        api.reset()

        collection = api.create_collection("all-my-documents")
        collection.add(
            documents=["This is document1", "This is document2"],
            # we handle tokenization, embedding, and indexing automatically. You can skip that and add your own embeddings as well
            metadatas=[{"source": "notion"}, {"source": "google-docs"}],  # filter on these!
            ids=["doc1", "doc2"],  # unique for each doc
        )

        results = collection.query(
            query_texts=["This is document1"],
            n_results=2,
            # where={"metadata_field": "is_equal_to_this"}, # optional filter
            # where_document={"$contains":"search_string"}  # optional filter
        )
        print(results)
