from chromadb.db import DB
from chromadb.db.index.hnswlib import Hnswlib
from chromadb.errors import NoDatapointsException
import uuid
import time
import os
import itertools
import json
from typing import Sequence, Any
import clickhouse_connect

COLLECTION_TABLE_SCHEMA = [{"uuid": "UUID"}, {"name": "String"}, {"metadata": "String"}]

EMBEDDING_TABLE_SCHEMA = [
    {"collection_uuid": "UUID"},
    {"uuid": "UUID"},
    {"embedding": "Array(Float64)"},
    {"document": "Nullable(String)"},
    {"id": "Nullable(String)"},
    {"metadata": "String"},
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

    #
    #  INIT METHODS
    #
    def __init__(self, settings):
        self._conn = clickhouse_connect.get_client(
            host=settings.clickhouse_host, port=int(settings.clickhouse_port)
        )
        self._conn.query(f"""SET allow_experimental_lightweight_delete = 1;""")
        self._conn.query(
            f"""SET mutations_sync = 1;"""
        )  # https://clickhouse.com/docs/en/operations/settings/settings/#mutations_sync

        self._create_table_collections()
        self._create_table_embeddings()
        self._idx = Hnswlib(settings)
        self._settings = settings

    def _create_table_collections(self):
        self._conn.command(
            f"""CREATE TABLE IF NOT EXISTS collections (
            {db_array_schema_to_clickhouse_schema(COLLECTION_TABLE_SCHEMA)}
        ) ENGINE = MergeTree() ORDER BY uuid"""
        )

    def _create_table_embeddings(self):
        self._conn.command(
            f"""CREATE TABLE IF NOT EXISTS embeddings (
            {db_array_schema_to_clickhouse_schema(EMBEDDING_TABLE_SCHEMA)}
        ) ENGINE = MergeTree() ORDER BY collection_uuid"""
        )

    #
    #  UTILITY METHODS
    #
    def get_collection_uuid_from_name(self, name):
        res = self._conn.query(
            f"""
            SELECT uuid FROM collections WHERE name = '{name}'
        """
        )
        return res.result_rows[0][0]

    def _create_where_clause(self, collection_uuid, ids=None, where={}):
        # ensure where only contains strings, as we only support string equality for now
        for key in where:
            if not isinstance(where[key], str):
                raise Exception("Invalid metadata: " + str(where))

        where = " AND ".join([self._filter_metadata(key, value) for key, value in where.items()])

        if ids is not None:
            # Check if where was created
            if len(where) > 6:  # NIT: hacky
                where += " AND "

            where += f" id IN {tuple(ids)}"

        where = f"WHERE {where}"

        if len(where) > 6:  # NIT: hacky
            where += " AND "

        where += f"collection_uuid = '{collection_uuid}'"
        return where

    #
    #  COLLECTION METHODS
    #
    def create_collection(self, name, metadata=None):
        if metadata is None:
            metadata = {}

        # poor man's unique constraint
        checkname = self._conn.query(
            f"""
            SELECT * FROM collections WHERE name = '{name}'
        """
        ).result_rows

        if len(checkname) > 0:
            raise Exception("Collection already exists with that name")

        collection_uuid = uuid.uuid4()
        data_to_insert = []
        data_to_insert.append([collection_uuid, name, json.dumps(metadata)])

        self._conn.insert("collections", data_to_insert, column_names=["uuid", "name", "metadata"])
        return collection_uuid

    def get_collection(self, name):
        return self._conn.query(
            f"""
         SELECT * FROM collections WHERE name = '{name}'
         """
        ).result_rows

    def list_collections(self) -> Sequence[Sequence[str]]:
        return self._conn.query(f"""SELECT * FROM collections""").result_rows

    def update_collection(self, current_name, new_name, new_metadata):
        if new_name is None:
            new_name = current_name
        if new_metadata is None:
            new_metadata = self.get_collection(current_name)[0]

        return self._conn.command(
            f"""

         ALTER TABLE 
            collections 
         UPDATE
            metadata = {new_metadata}, 
            name = '{new_name}'
         WHERE 

            name = '{current_name}'
         """
        )

    def delete_collection(self, name):
        collection_uuid = self.get_collection_uuid_from_name(name)
        self._conn.command(
            f"""
        DELETE FROM embeddings WHERE collection_uuid = '{collection_uuid}'
        """
        )

        self._conn.command(
            f"""
         DELETE FROM collections WHERE name = '{name}'
         """
        )

        self._idx.delete_index(collection_uuid)
        return True

    #
    #  ITEM METHODS
    #
    def add(self, collection_uuid, embedding, metadata=None, documents=None, ids=None):

        metadata = [json.dumps(x) if not isinstance(x, str) else x for x in metadata]

        data_to_insert = []
        for i in range(len(embedding)):
            data_to_insert.append(
                [collection_uuid, uuid.uuid4(), embedding[i], metadata[i], documents[i], ids[i]]
            )

        column_names = ["collection_uuid", "uuid", "embedding", "metadata", "document", "id"]
        self._conn.insert("embeddings", data_to_insert, column_names=column_names)

        return [x[1] for x in data_to_insert]  # return uuids

    def _get(self, where={}):
        return self._conn.query(
            f"""SELECT {db_schema_to_keys()} FROM embeddings {where}"""
        ).result_rows

    def _filter_metadata(self, key, value):
        return f" JSONExtractString(metadata,'{key}') = '{value}'"

    def get(
        self,
        where={},
        collection_name=None,
        collection_uuid=None,
        ids=None,
        sort=None,
        limit=None,
        offset=None,
    ):
        if collection_name == None and collection_uuid == None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        s3 = time.time()

        where = self._create_where_clause(collection_uuid, ids=ids, where=where)

        if sort is not None:
            where += f" ORDER BY {sort}"
        else:
            where += f" ORDER BY collection_uuid"  # stable ordering

        if limit is not None or isinstance(limit, int):
            where += f" LIMIT {limit}"

        if offset is not None or isinstance(offset, int):
            where += f" OFFSET {offset}"

        val = self._get(where=where)

        print(f"time to get {len(val)} embeddings: ", time.time() - s3)

        return val

    def _count(self, collection_uuid):
        where_string = ""
        if collection_uuid is not None:
            where_string = f"WHERE collection_uuid = '{collection_uuid}'"
        return self._conn.query(f"SELECT COUNT() FROM embeddings {where_string}").result_rows

    def count(self, collection_name):
        collection_uuid = self.get_collection_uuid_from_name(collection_name)
        return self._count(collection_uuid=collection_uuid)[0][0]

    def _delete(self, where_str=None):
        deleted_uuids = self._conn.query(f"""SELECT uuid FROM embeddings {where_str}""").result_rows
        self._conn.command(
            f"""
            DELETE FROM
                embeddings
        {where_str}
        """
        )
        return [res[0] for res in deleted_uuids] if len(deleted_uuids) > 0 else []

    def delete(self, where={}, collection_name=None, collection_uuid=None, ids=None):
        if collection_name == None and collection_uuid == None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        s3 = time.time()
        where_str = self._create_where_clause(collection_uuid, ids=ids, where=where)

        deleted_uuids = self._delete(where_str)
        print(f"time to get {len(deleted_uuids)} embeddings for deletion: ", time.time() - s3)

        # if len(where) == 1:
        #     self._idx.delete(collection_uuid)
        self._idx.delete_from_index(collection_uuid, deleted_uuids)

        return deleted_uuids

    def get_by_ids(self, ids: list):
        return self._conn.query(
            f"""
        SELECT {db_schema_to_keys()} FROM embeddings WHERE uuid IN ({[id.hex for id in ids]})
        """
        ).result_rows

    def get_nearest_neighbors(
        self, where, embeddings, n_results, collection_name=None, collection_uuid=None
    ) -> tuple[list[list[uuid.UUID]], list[list[float]]]:

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        results = self.get(collection_uuid=collection_uuid, where=where)

        if len(results) > 0:
            ids = [x[1] for x in results]
        else:
            raise NoDatapointsException("No datapoints found for the supplied filter")

        uuids, distances = self._idx.get_nearest_neighbors(
            collection_uuid, embeddings, n_results, ids
        )

        return uuids, distances

    def create_index(self, collection_uuid) -> None:
        """Create an index for a collection_uuid and optionally scoped to a dataset.
        Args:
            collection_uuid (str): The collection_uuid to create an index for
            dataset (str, optional): The dataset to scope the index to. Defaults to None.
        Returns:
            None
        """
        get = self.get(collection_uuid=collection_uuid)

        uuids = [x[1] for x in get]
        embeddings = [x[2] for x in get]

        self._idx.run(collection_uuid, uuids, embeddings)
        # chroma_telemetry.capture('created-index-run-process', {'n': len(get)})

    def add_incremental(self, collection_uuid, uuids, embeddings):
        self._idx.add_incremental(collection_uuid, uuids, embeddings)

    def has_index(self, collection_uuid):
        return self._idx.has_index(self, collection_uuid)

    def reset(self):
        self._conn.command("DROP TABLE collections")
        self._conn.command("DROP TABLE embeddings")
        self._create_table_collections()
        self._create_table_embeddings()

        self._idx.reset()
        self._idx = Hnswlib(self._settings)

    def raw_sql(self, sql):
        return self._conn.query(sql).result_rows
