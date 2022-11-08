from os import EX_CANTCREAT
from chroma_server.db.abstract import Database
import uuid
import time

from clickhouse_driver import connect, Client

EMBEDDING_TABLE_SCHEMA = [
    {'space_key': 'String'},
    {'uuid': 'UUID'},
    {'embedding_data': 'Array(Float64)'},
    {'input_uri': 'String'},
    {'dataset': 'String'},
    {'category_name': 'String'},
]

RESULTS_TABLE_SCHEMA = [
    {'space_key': 'String'},
    {'uuid': 'UUID'},
    {'custom_quality_score': ' Nullable(Float64)'},
]

def db_array_schema_to_clickhouse_schema(table_schema):
    return_str = ""
    for element in table_schema:
        for k, v in element.items():
            return_str += f"{k} {v}, "
    return return_str

def db_schema_to_keys():
    return_str = ""
    for element in EMBEDDING_TABLE_SCHEMA:
        if element == EMBEDDING_TABLE_SCHEMA[-1]:
            return_str += f"{list(element.keys())[0]}"
        else:
            return_str += f"{list(element.keys())[0]}, "
    return return_str

def get_col_pos(col_name):
    for i, col in enumerate(EMBEDDING_TABLE_SCHEMA):
        if col_name in col:
            return i

class Clickhouse(Database):
    _conn = None

    def _create_table_embeddings(self):
        self._conn.execute(f'''CREATE TABLE IF NOT EXISTS embeddings (
            {db_array_schema_to_clickhouse_schema(EMBEDDING_TABLE_SCHEMA)}
        ) ENGINE = MergeTree() ORDER BY space_key''')
    
    def _create_table_results(self):
        self._conn.execute(f'''CREATE TABLE IF NOT EXISTS results (
            {db_array_schema_to_clickhouse_schema(RESULTS_TABLE_SCHEMA)}
        ) ENGINE = MergeTree() ORDER BY space_key''')

    def __init__(self):
        client = Client('clickhouse')
        self._conn = client
        self._create_table_embeddings()
        self._create_table_results()

    def add_batch(self, space_key, embedding_data, input_uri, dataset=None, custom_quality_score=None, category_name=None):
        data_to_insert = []
        for i in range(len(embedding_data)):
            data_to_insert.append([space_key[i], uuid.uuid4(), embedding_data[i], input_uri[i], dataset[i], category_name[i]])

        self._conn.execute('''
         INSERT INTO embeddings (space_key, uuid, embedding_data, input_uri, dataset, category_name) VALUES''', data_to_insert)
        
    def count(self, space_key=None):
        where_string = ""
        if space_key:
            where_string = f"WHERE space_key = '{space_key}'"
        return self._conn.execute(f"SELECT COUNT() FROM embeddings {where_string}")[0][0]

    def fetch(self, where_filter={}, sort=None, limit=None, columnar=False):
        if where_filter["space_key"] is None:
            return {"error": "space_key is required"}

        s3= time.time()
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

        val = self._conn.execute(f'''
            SELECT 
                {db_schema_to_keys()}
            FROM 
                embeddings
        {where_filter}
        ''', columnar=columnar)
        print(f"time to fetch {len(val)} embeddings: ", time.time() - s3)

        return val

    def get_by_ids(self, ids=list):
        return self._conn.execute(f'''
            SELECT {db_schema_to_keys()} FROM embeddings WHERE uuid IN ({ids})''')

    def reset(self):
        self._conn.execute('DROP TABLE embeddings')
        self._create_table_embeddings()
        self._create_table_results()

    def raw_sql(self, sql):
        return self._conn.execute(sql)

    def add_results(self, space_keys, uuids, custom_quality_score):
        data_to_insert = []
        for i in range(len(space_keys)):
            data_to_insert.append([space_keys[i], uuids[i], custom_quality_score[i]])

        self._conn.execute('''
         INSERT INTO results (space_key, uuid, custom_quality_score) VALUES''', data_to_insert)
    
    def delete_results(self, space_key):
        self._conn.execute(f"DELETE FROM results WHERE space_key = '{space_key}'")
     
    def return_results(self, space_key, n_results = 100):
        return self._conn.execute(f'''
            SELECT
                embeddings.input_uri,
                embeddings.embedding_data,
                results.custom_quality_score
            FROM
                results
            INNER JOIN
                embeddings
            ON
                results.uuid = embeddings.uuid
            WHERE
                results.space_key = '{space_key}'
            ORDER BY
                results.custom_quality_score DESC
            LIMIT {n_results}
        ''')
