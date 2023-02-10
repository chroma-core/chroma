from chromadb.db import DB
from chromadb.db.index.hnswlib import Hnswlib
from chromadb.db.clickhouse import Clickhouse, db_array_schema_to_clickhouse_schema, EMBEDDING_TABLE_SCHEMA, db_schema_to_keys, COLLECTION_TABLE_SCHEMA
import pandas as pd
import numpy as np
import json
import duckdb
import uuid
import time
import itertools

def clickhouse_to_duckdb_schema(table_schema):
    for item in table_schema:
            if 'embedding' in item:
                item['embedding'] = 'DOUBLE[]'
            # capitalize the key
            item[list(item.keys())[0]] = item[list(item.keys())[0]].upper()
            if 'NULLABLE' in item[list(item.keys())[0]]:
                item[list(item.keys())[0]] = item[list(item.keys())[0]].replace('NULLABLE(', '').replace(')', '')
            if 'UUID' in item[list(item.keys())[0]]:
                item[list(item.keys())[0]] = 'STRING'
            if 'FLOAT64' in item[list(item.keys())[0]]:
                item[list(item.keys())[0]] = 'DOUBLE'
            # NIT: here we need to turn metadata into JSON for duckdb

    return table_schema


# TODO: inherits ClickHouse for convenience of copying behavior, not
# because it's logically a subtype. Factoring out the common behavior
# to a third superclass they both extend would be preferable.
class DuckDB(Clickhouse):


    # duckdb has a different way of connecting to the database
    def __init__(self, settings):
        self._conn = duckdb.connect()
        self._create_table_collections()
        self._create_table_embeddings()
        self._idx = Hnswlib(settings)
        self._settings = settings

        # https://duckdb.org/docs/extensions/overview
        self._conn.execute("INSTALL 'json';")
        self._conn.execute("LOAD 'json';")

    def _create_table_collections(self):
        self._conn.execute(f'''CREATE TABLE collections (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(COLLECTION_TABLE_SCHEMA))}
        ) ''')


    # duckdb has different types, so we want to convert the clickhouse schema to duckdb schema
    def _create_table_embeddings(self):
        self._conn.execute(f'''CREATE TABLE embeddings (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(EMBEDDING_TABLE_SCHEMA))}
        ) ''')

    # 
    #  UTILITY METHODS
    # 
    def get_collection_uuid_from_name(self, name):
        return self._conn.execute(f'''SELECT uuid FROM collections WHERE name = ?''', [name]).fetchall()[0][0]

   
    # 
    #  COLLECTION METHODS
    # 
    def create_collection(self, name, metadata=None):
        if metadata is None:
            metadata = {}

        # poor man's unique constraint
        if not self.get_collection(name).empty:
            raise Exception(f'collection with name {name} already exists')

        return self._conn.execute(f'''INSERT INTO collections (uuid, name, metadata) VALUES (?, ?, ?)''', [str(uuid.uuid4()), name, json.dumps(metadata)])

    def get_collection(self, name):
        return self._conn.execute(f'''SELECT * FROM collections WHERE name = ?''', [name]).df()
    
    def list_collections(self):
        return self._conn.execute(f'''SELECT * FROM collections''').fetchall()

    def update_collection(self, name, metadata):
        return self._conn.execute(f'''UPDATE collections SET metadata = ? WHERE name = ?''', [json.dumps(metadata), name])

    def delete_collection(self, name):
        return self._conn.execute(f'''DELETE FROM collections WHERE name = ?''', [name])


    # 
    #  ITEM METHODS
    # 
    # the execute many syntax is different than clickhouse, the (?,?) syntax is different than clickhouse
    def add(self, collection_uuid, embedding, metadata=None, documents=None, ids=None):

        metadata = [json.dumps(x) if not isinstance(x, str) else x for x in metadata]
        
        data_to_insert = []
        for i in range(len(embedding)):
            data_to_insert.append([collection_uuid, str(uuid.uuid4()), embedding[i], metadata[i], documents[i], ids[i]])

        insert_string = "collection_uuid, uuid, embedding, metadata, document, id"

        self._conn.executemany(f'''
         INSERT INTO embeddings ({insert_string}) VALUES (?,?,?,?,?,?)''', data_to_insert)
        
        return [uuid.UUID(x[1]) for x in data_to_insert] # return uuids


    def _count(self, collection_uuid):
        where_string = f"WHERE collection_uuid = '{collection_uuid}'"
        return self._conn.query(f"SELECT COUNT() FROM embeddings {where_string}")


    def count(self, collection_name=None):
        collection_uuid = self.get_collection_uuid_from_name(collection_name)
        return self._count(collection_uuid=collection_uuid).fetchall()[0][0]


    def _filter_metadata(self, key, value):
        return f" json_extract_string(metadata,'$.{key}') = '{value}'"


    def _get(self, where):
        val = self._conn.execute(f'''SELECT {db_schema_to_keys()} FROM embeddings {where}''').fetchall()
        for i in range(len(val)):
            val[i] = list(val[i])
            val[i][0] = uuid.UUID(val[i][0])
            val[i][1] = uuid.UUID(val[i][1])

        return val


    def _delete(self, where_str):
        uuids_deleted = self._conn.execute(f'''SELECT uuid FROM embeddings {where_str}''').fetchall()
        self._conn.execute(f'''
            DELETE FROM
                embeddings
        {where_str}
        ''').fetchall()[0]
        return [uuid.UUID(x[0]) for x in uuids_deleted]


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
        ''').df()


    def raw_sql(self, sql):
        return self._conn.execute(sql).df()


    # TODO: This method should share logic with clickhouse impl
    def reset(self):
        self._conn.execute('DROP TABLE collections')
        self._conn.execute('DROP TABLE embeddings')
        self._create_table_collections()
        self._create_table_embeddings()
        
        self._idx.reset()
        self._idx = Hnswlib(self._settings)


class PersistentDuckDB(DuckDB):

    _save_folder = None

    def __init__(self, settings):
        super().__init__(settings=settings)
        self._save_folder = settings.chroma_cache_dir
        self.load()


    def set_save_folder(self, path):
        self._save_folder = path


    def get_save_folder(self):
        return self._save_folder


    def persist(self):
        '''
        Persist the database to disk
        '''
        print("Persisting DB to disk, putting it in the save folder", self._save_folder)
        if self._conn is None:
            return

        # if the db is empty, dont save
        if self.count() == 0:
            return

        self._conn.execute(f'''
            COPY
                (SELECT * FROM embeddings)
            TO '{self._save_folder}/chroma.parquet'
                (FORMAT PARQUET);
        ''')


    def load(self):
        '''
        Load the database from disk
        '''
        import os

        # load in the embeddings
        if not os.path.exists(f"{self._save_folder}/chroma.parquet"):
            print(f"No existing DB found in {self._save_folder}, skipping load")
        else:
            path = self._save_folder + "/chroma.parquet"
            self._conn.execute(f"INSERT INTO embeddings SELECT * FROM read_parquet('{path}');")


    def __del__(self):
        print("PersistentDuckDB del, about to run persist")
        self.persist()
