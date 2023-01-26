from chroma.db import DB
from chroma.db.index.hnswlib import Hnswlib
from chroma.db.clickhouse import Clickhouse, db_array_schema_to_clickhouse_schema, EMBEDDING_TABLE_SCHEMA, db_schema_to_keys#, RESULTS_TABLE_SCHEMA
import pandas as pd
import numpy as np
import duckdb
import uuid
import time
import itertools

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


# TODO: inherits ClickHouse for convenience of copying behavior, not
# because it's logically a subtype. Factoring out the common behavior
# to a third superclass they both extend would be preferable.
class DuckDB(Clickhouse):


    # duckdb has different types, so we want to convert the clickhouse schema to duckdb schema
    def _create_table_embeddings(self):
        self._conn.execute(f'''CREATE TABLE embeddings (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(EMBEDDING_TABLE_SCHEMA))}
        ) ''')

    # def _create_table_results(self):
    #     self._conn.execute(f'''CREATE TABLE results (
    #         {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(RESULTS_TABLE_SCHEMA))}
    #     ) ''')


    # duckdb has a different way of connecting to the database
    def __init__(self, settings):
        self._conn = duckdb.connect()
        self._create_table_embeddings()
        # self._create_table_results()
        self._idx = Hnswlib(settings)
        self._settings = settings

        # https://duckdb.org/docs/extensions/overview
        self._conn.execute("INSTALL 'json';")
        self._conn.execute("LOAD 'json';")


    # the execute many syntax is different than clickhouse, the (?,?) syntax is different than clickhouse
    def add(self, model_space, embedding, input_uri, dataset=None, metadata=None):#, inference_class=None, label_class=None):
        data_to_insert = []
        for i in range(len(embedding)):
            data_to_insert.append([model_space[i], str(uuid.uuid4()), embedding[i], input_uri[i], dataset[i], metadata[i]])#, inference_class[i], (label_class[i] if label_class is not None else None)])

        insert_string = "model_space, uuid, embedding, input_uri, dataset, metadata"#, inference_class, label_class"
        self._conn.executemany(f'''
         INSERT INTO embeddings ({insert_string}) VALUES (?,?,?,?,?,?)''', data_to_insert)


    def count(self, model_space=None):
        return self._count(model_space=model_space).fetchall()[0][0]


    def _fetch(self, where=""):
        val = self._conn.execute(f'''SELECT {db_schema_to_keys()} FROM embeddings {where}''').df()
        # Convert UUID strings to UUID objects
        val['uuid'] = val['uuid'].apply(lambda x: uuid.UUID(x))
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
    
    # def delete_results(self, model_space):
    #     self._conn.execute(f"DELETE FROM results WHERE model_space = '{model_space}'")

    # def add_results(self, uuid, model_space, **kwargs):

    #     # Make sure the kwarg keys are in the results table schema
    #     results_table_cols = {list(col.keys())[0] for col in RESULTS_TABLE_SCHEMA}
    #     results_cols = set(kwargs.keys())
    #     results_cols.update(['uuid', 'model_space'])

    #     if not (results_table_cols == results_cols):
    #         if not results_table_cols.issuperset(results_cols):
    #             raise Exception(f"Invalid results columns: {results_cols - results_table_cols}")
    #         else:
    #             # Log a warning
    #             print(f"Warning: results missing columns: {results_table_cols - results_cols}")

    #     data_to_insert = list(zip(itertools.repeat(model_space), uuid, *kwargs.values()))
    #     # convert numpy floats to python floats
    #     data_to_insert = [[x[0], x[1], *[float(y) for y in x[2:]]] for x in data_to_insert]
    #     question_marks = ", ".join(["?"] * len(kwargs.keys()))

    #     self._conn.executemany(f'''
    #      INSERT INTO results (model_space, uuid, {",".join(kwargs.keys())}) VALUES (?,?, {question_marks})''', data_to_insert)

    # def get_results_by_column(self, column_name: str, model_space: str, n_results: int, sort: str = 'ASC'):
    #     return self._conn.execute(f'''
    #         SELECT
    #             embeddings.input_uri,
    #             results.{column_name}
    #         FROM
    #             results
    #         INNER JOIN
    #             embeddings
    #         ON
    #             results.uuid = embeddings.uuid
    #         WHERE
    #             results.model_space = '{model_space}'
    #         ORDER BY
    #             results.{column_name} {sort}
    #         LIMIT {n_results}
    #     ''').df()

    def get_random(self, where={}, n=1):
        # check to see if query is a dict and if it is a flat list of key value pairs
        if where is not None:
            if not isinstance(where, dict):
                raise Exception("Invalid where: " + str(where))

            # ensure where is a flat dict
            for key in where:
                if isinstance(where[key], dict):
                    raise Exception("Invalid where: " + str(where))

        where = " AND ".join([f"{key} = '{value}'" for key, value in where.items()])
        if where:
            where = f"WHERE {where}"

        return self._conn.execute(f'''
            SELECT {db_schema_to_keys()} FROM embeddings {where} LIMIT {n}''').df() # ORDER BY rand()


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
        # self._conn.execute(f'''
        #     COPY
        #         (SELECT * FROM results)
        #     TO '{self._save_folder}/chroma_results.parquet'
        #         (FORMAT PARQUET);
        # ''')


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

        # load in the results
        # if not os.path.exists(f"{self._save_folder}/chroma_results.parquet"):
        #     pass
        # else:
        #     path = self._save_folder + "/chroma_results.parquet"
        #     self._conn.execute(f"INSERT INTO results SELECT * FROM read_parquet('{path}');")


    def __del__(self):
        print("PersistentDuckDB del, about to run persist")
        self.persist()
