import duckdb
import uuid
import time
from chroma_server.db.clickhouse import Clickhouse, db_array_schema_to_clickhouse_schema, EMBEDDING_TABLE_SCHEMA, RESULTS_TABLE_SCHEMA, db_schema_to_keys
import pandas as pd
import numpy as np

def clickhouse_to_duckdb_schema(table_schema):
    for item in table_schema:
            if 'embedding' in item:
                item['embedding'] = 'REAL[]'
            # capitalize the key
            item[list(item.keys())[0]] = item[list(item.keys())[0]].upper()
            if 'NULLABLE' in item[list(item.keys())[0]]:
                item[list(item.keys())[0]] = item[list(item.keys())[0]].replace('NULLABLE(', '').replace(')', '')
            if 'UUID' in item[list(item.keys())[0]]:
                item[list(item.keys())[0]] = 'STRING'
            if 'FLOAT64' in item[list(item.keys())[0]]:
                item[list(item.keys())[0]] = 'REAL'

    return table_schema

class DuckDB(Clickhouse):

    # duckdb has different types, so we want to convert the clickhouse schema to duckdb schema
    def _create_table_embeddings(self):
        self._conn.execute(f'''CREATE TABLE embeddings (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(EMBEDDING_TABLE_SCHEMA))}
        ) ''')
    
    def _create_table_results(self):
        self._conn.execute(f'''CREATE TABLE results (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(RESULTS_TABLE_SCHEMA))}
        ) ''')

    # duckdb has a different way of connecting to the database
    def __init__(self):
        self._conn = duckdb.connect()
        self._create_table_embeddings()
        self._create_table_results()

    # the execute many syntax is different than clickhouse, the (?,?) syntax is different than clickhouse
    def add(self, model_space, embedding, input_uri, dataset=None, inference_class=None, label_class=None):
        data_to_insert = []
        for i in range(len(embedding)):
            data_to_insert.append([model_space[i], str(uuid.uuid4()), embedding[i], input_uri[i], dataset[i], inference_class[i], (label_class[i] if label_class is not None else None)])
        
        insert_string = "model_space, uuid, embedding, input_uri, dataset, inference_class, label_class"
        self._conn.executemany(f'''
         INSERT INTO embeddings ({insert_string}) VALUES (?,?,?,?,?,?,?)''', data_to_insert)

    def count(self, model_space=None):
        return self._count(model_space=model_space).fetchall()[0][0]

    def _fetch(self, where={}):
        val = self._conn.execute(f'''SELECT {db_schema_to_keys()} FROM embeddings {where}''').df()
        return val

    def _delete(self, where={}):
        uuids_deleted = self._conn.execute(f'''SELECT uuid FROM embeddings {where}''').fetchall()
        self._conn.execute(f'''
            DELETE FROM 
                embeddings
        {where}
        ''').fetchall()[0]
        return uuids_deleted

    def get_by_ids(self, ids=list):
        # select from duckdb table where ids are in the list
        if not isinstance(ids, list):
            raise Exception("ids must be a list")
        
        if not ids:
            # create an empty pandas dataframe
            return pd.DataFrame()

        return self._conn.execute(f'''
            SELECT 
                {db_schema_to_keys()}
            FROM 
                embeddings
            WHERE
                uuid IN ({','.join([("'" + str(x) + "'") for x in ids])})
        ''').fetchall()