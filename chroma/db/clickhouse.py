from chroma.db import DB
from chroma.db.index.hnswlib import Hnswlib
from chroma.errors import NoDatapointsException
import uuid
import time
import os
import itertools

from clickhouse_driver import connect, Client

EMBEDDING_TABLE_SCHEMA = [
    {'model_space': 'String'},
    {'uuid': 'UUID'},
    {'embedding': 'Array(Float64)'},
    {'input_uri': 'String'},
    {'dataset': 'String'},
    {'metadata': 'Map(String, String)'}
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


class Clickhouse(DB):

    _conn = None

    def _create_table_embeddings(self):
        self._conn.execute(f'''CREATE TABLE IF NOT EXISTS embeddings (
            {db_array_schema_to_clickhouse_schema(EMBEDDING_TABLE_SCHEMA)}
        ) ENGINE = MergeTree() ORDER BY model_space''')

        self._conn.execute(f'''SET allow_experimental_lightweight_delete = true''')
        self._conn.execute(f'''SET mutations_sync = 1''') # https://clickhouse.com/docs/en/operations/settings/settings/#mutations_sync


    def __init__(self, settings):
        self._conn = Client(host=settings.clickhouse_host, port=settings.clickhouse_port)
        self._create_table_embeddings()
        self._idx = Hnswlib(settings)
        self._settings = settings

    def add(self, model_space, embedding, input_uri, dataset=None, metadata=None):
        data_to_insert = []
        for i in range(len(embedding)):
            data_to_insert.append([model_space[i], uuid.uuid4(), embedding[i], input_uri[i], dataset[i], metadata[i]])

        print("data_to_insert", data_to_insert[0])

        insert_string = "model_space, uuid, embedding, input_uri, dataset, metadata"

        self._conn.execute(f'''
         INSERT INTO embeddings ({insert_string}) VALUES''', data_to_insert)


    def _fetch(self, where={}):
        return self._conn.query_dataframe(f'''SELECT {db_schema_to_keys()} FROM embeddings {where}''')

    def _filter_metadata(self, key, value):
        return f" AND metadata['{key}'] = '{value}'"

    def fetch(self, where={}, sort=None, limit=None, offset=None):
        if where["model_space"] is None:
            return {"error": "model_space is required"}

        s3= time.time()
        # check to see if query is a dict and if it is a flat list of key value pairs
        if where is not None:
            if not isinstance(where, dict):
                raise Exception("Invalid where: " + str(where))

            # ensure where is a flat dict - otherwise we cant use clickhouse Map (and JSON current not supported in clickhouse-driver)
            # for key in where:
            #     if isinstance(where[key], dict):
            #         raise Exception("Invalid where: " + str(where))

        metadata_query = None
        # if where has a metadata key, we need to do a special query
        if "metadata" in where:
            metadata_query = where["metadata"]
            del where["metadata"]

        where = " AND ".join([f"{key} = '{value}'" for key, value in where.items()])
        if metadata_query is not None:
            for key, value in metadata_query.items():
                where += self._filter_metadata(key, value)
        
        if where:
            where = f"WHERE {where}"

        if sort is not None:
            where += f" ORDER BY {sort}"
        else:
            where += f" ORDER BY model_space" # stable ordering

        if limit is not None or isinstance(limit, int):
            where += f" LIMIT {limit}"

        if offset is not None or isinstance(offset, int):
            where += f" OFFSET {offset}"

        val = self._fetch(where=where)

        print(f"time to fetch {len(val)} embeddings: ", time.time() - s3)

        return val


    def _count(self, model_space=None):
        where_string = ""
        if model_space is not None:
            where_string = f"WHERE model_space = '{model_space}'"
        return self._conn.execute(f"SELECT COUNT() FROM embeddings {where_string}")


    def count(self, model_space=None):
        return self._count(model_space=model_space)[0][0]


    def _delete(self, where_str=None):
        uuids_deleted = self._conn.query_dataframe(f'''SELECT uuid FROM embeddings {where_str}''')

        self._conn.execute(f'''
            DELETE FROM
                embeddings
        {where_str}
        ''')
        return uuids_deleted.uuid.tolist() if len(uuids_deleted) > 0 else []


    def delete(self, where={}):
        if where["model_space"] is None:
            return {"error": "model_space is required. Use reset to clear the entire db"}

        s3= time.time()
        # check to see if query is a dict and if it is a flat list of key value pairs
        if where is not None:
            if not isinstance(where, dict):
                raise Exception("Invalid where: " + str(where))

            # ensure where is a flat dict
            for key in where:
                if isinstance(where[key], dict):
                    raise Exception("Invalid where: " + str(where))

        where_str = " AND ".join([f"{key} = '{value}'" for key, value in where.items()])

        if where_str:
            where_str = f"WHERE {where_str}"
        deleted_uuids = self._delete(where_str)
        print(f"time to fetch {len(deleted_uuids)} embeddings for deletion: ", time.time() - s3)

        if len(where) == 1:
            self._idx.delete(where['model_space'])
        else:
            self._idx.delete_from_index(where['model_space'], deleted_uuids)

        return deleted_uuids


    def get_by_ids(self, ids=list):
        df = self._conn.query_dataframe(f'''
        SELECT {db_schema_to_keys()} FROM embeddings WHERE uuid IN ({[id.hex for id in ids]})
        ''')
        return df


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

        return self._conn.query_dataframe(f'''
            SELECT {db_schema_to_keys()} FROM embeddings {where} ORDER BY rand() LIMIT {n}''')


    def get_nearest_neighbors(self, where, embedding, n_results):

        results = self.fetch(where)
        if len(results) > 0:
            ids = results.uuid.tolist()
        else:
            raise NoDatapointsException("No datapoints found for the supplied filter")

        uuids, distances = self._idx.get_nearest_neighbors(where['model_space'], embedding, n_results, ids)

        return {
            "ids": uuids,
            "embeddings": self.get_by_ids(uuids[0]),
            "distances": distances.tolist()[0]
        }

    def create_index(self, model_space: str, dataset_name: str=None) -> None:
        """Create an index for a model_space and optionally scoped to a dataset. 
        Args:
            model_space (str): The model_space to create an index for
            dataset (str, optional): The dataset to scope the index to. Defaults to None.
        Returns:
            None
        """
        query = {"model_space": model_space}
        if dataset_name is not None:
            query["dataset"] = dataset_name
        fetch = self.fetch(query)
        self._idx.run(model_space, fetch.uuid.tolist(), fetch.embedding.tolist())
        #chroma_telemetry.capture('created-index-run-process', {'n': len(fetch)})


    def has_index(self, model_space):
        return self._idx.has_index(self, model_space)


    def reset(self):
        self._conn.execute('DROP TABLE embeddings')
        self._create_table_embeddings()

        self._idx.reset()
        self._idx = Hnswlib(self._settings)


    def raw_sql(self, sql):
        return self._conn.query_dataframe(sql)

