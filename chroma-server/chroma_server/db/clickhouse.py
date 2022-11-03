from os import EX_CANTCREAT
from chroma_server.db.abstract import Database
import uuid

from clickhouse_driver import connect, Client

class Clickhouse(Database):
    _conn = None

    def _create_table_embeddings(self):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS embeddings (
            uuid UUID,
            embedding_data Array(Float64),
            input_uri String,
            dataset String,
            custom_quality_score Nullable(Float64),
            category_name String,
        )  ENGINE = Memory''')

    def __init__(self):
        # https://stackoverflow.com/questions/59224272/connect-cannot-assign-requested-address
        client = Client('clickhouse')
        self._conn = client
        self._create_table_embeddings()

    def add_batch(self, embedding_data, input_uri, dataset=None, custom_quality_score=None, category_name=None):

        data_to_insert = []
        for i in range(len(embedding_data)):
            data_to_insert.append([uuid.uuid4(), embedding_data[i], input_uri[i], dataset[i], category_name[i]])

        self._conn.execute('''
         INSERT INTO embeddings (uuid, embedding_data, input_uri, dataset, category_name) VALUES''', data_to_insert)
        
    def count(self):
        return self._conn.execute('SELECT COUNT() FROM embeddings')

    def update(self, data): # call this update_custom_quality_score! that is all it does
        pass

    def fetch(self, where_filter={}, sort=None, limit=None):
        # check to see if query is a dict and if it is a flat list of key value pairs
        if where_filter is not None:
            if not isinstance(where_filter, dict):
                raise Exception("Invalid where_filter: " + str(where_filter))
            
            # ensure where_filter is a flat dict
            for key in where_filter:
                if isinstance(where_filter[key], dict):
                    raise Exception("Invalid where_filter: " + str(where_filter))
        
        where_filter = " AND ".join([f"{key} = '{value}'" for key, value in where_filter.items()])

        if where_filter:
            where_filter = f"WHERE {where_filter}"

        if sort is not None:
            where_filter += f" ORDER BY {sort}"

        if limit is not None or isinstance(limit, int):
            where_filter += f" LIMIT {limit}"

        return self._conn.execute(f'''
            SELECT 
                uuid,
                embedding_data, 
                input_uri,
                dataset,
                custom_quality_score,
                category_name
            FROM 
                embeddings
        {where_filter}
        ''')

    def delete_batch(self, batch):
        pass

    def persist(self):
        pass

    def load(self, path=".chroma/chroma.parquet"):
        pass

    def get_by_ids(self, ids=list):
        return self._conn.execute(f'''
            SELECT 
                uuid,
                embedding_data, 
                input_uri,
                dataset,
                custom_quality_score,
                category_name
            FROM 
                embeddings
            WHERE
                uuid IN ({ids})
        ''')

    def reset(self):
        self._conn.execute('DROP TABLE embeddings')
        self._create_table_embeddings()