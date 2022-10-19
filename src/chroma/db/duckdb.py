from db.abstract import Database
import duckdb

class DuckDB(Database):
    _conn = None

    def __init__(self):
        self._conn = duckdb.connect()
        self._conn.execute('''
        CREATE TABLE embeddings (embedding_data REAL[], metadata STRUCT(i VARCHAR, j VARCHAR), input_uri STRING, inference_data STRUCT(i VARCHAR, j VARCHAR))
        ''')
        return

    def add_batch(self, embedding_data, metadata, input_uri, inference_data):
        # TODO: extend this to check if everything is a list and has the same length, if so, then use executemany
        self._conn.execute("INSERT INTO embeddings VALUES (?, ?, ?, ?)", [embedding_data, metadata, input_uri, inference_data])
        # self._conn.executemany("INSERT INTO test_table VALUES (?, ?)”, [[3, 'three'], [4, 'four']]")
        return

    def fetch(self, query):
        self._conn.execute("SELECT * from embeddings ").fetchdf()
        return

    def delete_batch(self, batch):
        # DELETE FROM tbl WHERE i=2;
        # TODO: implement
        return

    def persist(self):
        # TODO: implement
        # COPY (SELECT * FROM tbl) TO 'output.parquet' (FORMAT PARQUET);
        return

    def load(self):
        # TODO: implement
        # SELECT * FROM 'test.parquet';
        return  