import json
import numpy
import sqlite3

from collections import defaultdict
from contextlib import closing
from pathlib import Path

class SQLitedb:
    training_embeddings = defaultdict(list)
    prod_embeddings = []

    sql = {}

    @property
    def db_path(self):
        return self.scratch_path / "sqlite.db"

    @property
    def sql_path(self):
        return Path(__file__).parent / "sqlite_sql"

    def load_sql(self):
        for query in self.sql_path.glob("*.sql"):
            self.sql[query.stem] = self.read_sql_path(query)
    
    def read_sql_path(self, path):
        with open(path, 'r') as file:
            return file.read()
        
    def init_db(self, scratch_path):
        self.scratch_path = Path(scratch_path)
        self.load_sql()
        self.db_path.unlink(missing_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self.create_tables()

    def create_tables(self):
        with self.connection:
            with closing(self.connection.cursor()) as cursor:
                cursor.execute(self.sql['create_train'])
                cursor.execute(self.sql['create_prod'])

    def prod_fields(self, embedding):
        return [
            json.dumps(embedding.data),
            json.dumps(embedding.inferences),
            json.dumps(embedding.labels),
            'project',
            'model',
            'layer',
            embedding.resource_uri,
        ]
                
    def ingest_prod(self, embedding):
        with self.connection:
            with closing(self.connection.cursor()) as cursor:
                result = cursor.execute(self.sql['insert_prod'], self.prod_fields(embedding))

    def training_fields(self, embedding):
        return [
            json.dumps(embedding.data),
            json.dumps(embedding.inferences),
            json.dumps(embedding.labels),
            'project',
            'model',
            'layer',
            embedding.resource_uri,
        ]

    def ingest_training(self, embedding):
        with self.connection:
            with closing(self.connection.cursor()) as cursor:
                for category in embedding.inferences:
                    result = cursor.execute(self.sql['insert_prod'], self.training_fields(embedding))

    def training_counts(self):
        return [(cat, len(embeds)) for cat, embeds in self.training_embeddings.items()]

    def categories(self):
        return self.training_embeddings.keys()

    def embeddings_for_category(self, category):
        return self.training_embeddings[category]

    
if __name__ == "__main__":
    test = SQLitedb()
    test.init_db("/tmp")
    print("init")
    test.ingest_prod('')
