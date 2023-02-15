from chromadb.api.types import Documents, Embeddings, IDs, Metadatas, Where, WhereDocument
from chromadb.db import DB
from chromadb.db.index.hnswlib import Hnswlib
from chromadb.errors import NoDatapointsException
import uuid
import time
import os
import itertools
import json
from typing import Optional, Sequence, List, Tuple, cast
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
        self._conn = None
        self._idx = Hnswlib(settings)
        self._settings = settings

    def _init_conn(self):
        self._conn = clickhouse_connect.get_client(
            host=self._settings.clickhouse_host, port=int(self._settings.clickhouse_port)
        )
        self._conn.query(f"""SET allow_experimental_lightweight_delete = 1;""")
        self._conn.query(
            f"""SET mutations_sync = 1;"""
        )  # https://clickhouse.com/docs/en/operations/settings/settings/#mutations_sync
        self._create_table_collections(self._conn)
        self._create_table_embeddings(self._conn)

    def _get_conn(self):
        if self._conn is None:
            self._init_conn()
        return self._conn

    def _create_table_collections(self, conn):
        conn.command(
            f"""CREATE TABLE IF NOT EXISTS collections (
            {db_array_schema_to_clickhouse_schema(COLLECTION_TABLE_SCHEMA)}
        ) ENGINE = MergeTree() ORDER BY uuid"""
        )

    def _create_table_embeddings(self, conn):
        conn.command(
            f"""CREATE TABLE IF NOT EXISTS embeddings (
            {db_array_schema_to_clickhouse_schema(EMBEDDING_TABLE_SCHEMA)}
        ) ENGINE = MergeTree() ORDER BY collection_uuid"""
        )

    #
    #  UTILITY METHODS
    #
    def persist(self):
        raise NotImplementedError("Clickhouse is a persistent database, this method is not needed")

    def get_collection_uuid_from_name(self, name):
        res = self._get_conn().query(
            f"""
            SELECT uuid FROM collections WHERE name = '{name}'
        """
        )
        return res.result_rows[0][0]

    def _create_where_clause(self, collection_uuid, ids=None, where={}, where_document={}):
        where_clauses = []
        self._format_where(where, where_clauses)
        if len(where_document) > 0:
            where_document_clauses = []
            self._format_where_document(where_document, where_document_clauses)
            where_clauses.extend(where_document_clauses)

        if ids is not None:
            where_clauses.append(f" id IN {tuple(ids)}")

        where_clauses.append(f"collection_uuid = '{collection_uuid}'")
        # We know that where_clauses is not empty, so force typechecker
        where = " AND ".join(cast(list[str], where_clauses))
        where = f"WHERE {where}"
        return where

    #
    #  COLLECTION METHODS
    #
    def create_collection(self, name, metadata=None):
        if metadata is None:
            metadata = {}

        # poor man's unique constraint
        checkname = (
            self._get_conn()
            .query(
                f"""
            SELECT * FROM collections WHERE name = '{name}'
        """
            )
            .result_rows
        )

        if len(checkname) > 0:
            raise Exception("Collection already exists with that name")

        collection_uuid = uuid.uuid4()
        data_to_insert = []
        data_to_insert.append([collection_uuid, name, json.dumps(metadata)])

        self._get_conn().insert(
            "collections", data_to_insert, column_names=["uuid", "name", "metadata"]
        )
        return collection_uuid

    def get_collection(self, name):
        return (
            self._get_conn()
            .query(
                f"""
         SELECT * FROM collections WHERE name = '{name}'
         """
            )
            .result_rows
        )

    def list_collections(self) -> Sequence[Sequence[str]]:
        return self._get_conn().query(f"""SELECT * FROM collections""").result_rows

    def update_collection(self, current_name, new_name, new_metadata):
        if new_name is None:
            new_name = current_name
        if new_metadata is None:
            new_metadata = self.get_collection(current_name)[0]

        return self._get_conn().command(
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
        self._get_conn().command(
            f"""
        DELETE FROM embeddings WHERE collection_uuid = '{collection_uuid}'
        """
        )

        self._get_conn().command(
            f"""
         DELETE FROM collections WHERE name = '{name}'
         """
        )

        self._idx.delete_index(collection_uuid)
        return True

    #
    #  ITEM METHODS
    #
    def add(self, collection_uuid, embedding, metadata, documents, ids):

        data_to_insert = []
        for i in range(len(embedding)):
            data_to_insert.append(
                [
                    collection_uuid,
                    uuid.uuid4(),
                    embedding[i],
                    json.dumps(metadata[i]),
                    documents[i],
                    ids[i],
                ]
            )

        column_names = ["collection_uuid", "uuid", "embedding", "metadata", "document", "id"]
        self._get_conn().insert("embeddings", data_to_insert, column_names=column_names)

        return [x[1] for x in data_to_insert]  # return uuids

    def _update(
        self,
        collection_uuid,
        ids: IDs,
        embeddings: Optional[Embeddings],
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
    ):

        updates = []
        parameters = {}
        for i in range(len(ids)):
            update_fields = []
            parameters[f"i{i}"] = ids[i]
            if embeddings is not None:
                update_fields.append(f"embedding = {{e{i}:Array(Float64)}}")
                parameters[f"e{i}"] = embeddings[i]
            if metadatas is not None:
                update_fields.append(f"metadata = {{m{i}:String}}")
                parameters[f"m{i}"] = json.dumps(metadatas[i])
            if documents is not None:
                update_fields.append(f"document = {{d{i}:String}}")
                parameters[f"d{i}"] = documents[i]

            update_statement = f"""
            UPDATE 
                {",".join(update_fields)}
            WHERE
                id = {{i{i}:String}} AND 
                collection_uuid = '{collection_uuid}'{"" if i == len(ids) - 1 else ","}
            """
            updates.append(update_statement)

        update_clauses = ("").join(updates)
        self._get_conn().command(f"ALTER TABLE embeddings {update_clauses}", parameters=parameters)

    def update(
        self,
        collection_uuid,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ):

        # Verify all IDs exist
        existing_items = self.get(collection_uuid=collection_uuid, ids=ids)
        if len(existing_items) != len(ids):
            raise ValueError("Some of the supplied ids for update were not found")

        # Update the db
        self._update(collection_uuid, ids, embeddings, metadatas, documents)

        # Update the index
        if embeddings is not None:
            update_uuids = [x[1] for x in existing_items]
            self._idx.delete_from_index(collection_uuid, update_uuids)
            self._idx.add_incremental(collection_uuid, update_uuids, embeddings)

    def _get(self, where={}):
        res = (
            self._get_conn()
            .query(f"""SELECT {db_schema_to_keys()} FROM embeddings {where}""")
            .result_rows
        )
        # json.load the metadata
        return [[*x[:5], json.loads(x[5])] for x in res]

    def _format_where(self, where, result):
        for key, value in where.items():
            # Shortcut for $eq
            if type(value) == str:
                result.append(f" JSONExtractString(metadata,'{key}') = '{value}'")
            elif type(value) == int:
                result.append(f" JSONExtractInt(metadata,'{key}') = {value}")
            elif type(value) == float:
                result.append(f" JSONExtractFloat(metadata,'{key}') = {value}")
            # Operator expression
            elif type(value) == dict:
                operator, operand = list(value.items())[0]
                if operator == "$gt":
                    return result.append(f" JSONExtractFloat(metadata,'{key}') > {operand}")
                elif operator == "$lt":
                    return result.append(f" JSONExtractFloat(metadata,'{key}') < {operand}")
                elif operator == "$gte":
                    return result.append(f" JSONExtractFloat(metadata,'{key}') >= {operand}")
                elif operator == "$lte":
                    return result.append(f" JSONExtractFloat(metadata,'{key}') <= {operand}")
                elif operator == "$ne":
                    if type(operand) == str:
                        return result.append(f" JSONExtractString(metadata,'{key}') != '{operand}'")
                    return result.append(f" JSONExtractFloat(metadata,'{key}') != {operand}")
                elif operator == "$eq":
                    if type(operand) == str:
                        return result.append(f" JSONExtractString(metadata,'{key}') = '{operand}'")
                    return result.append(f" JSONExtractFloat(metadata,'{key}') = {operand}")
                else:
                    raise ValueError(f"Operator {operator} not supported")
            elif type(value) == list:
                all_subresults = []
                for subwhere in value:
                    subresults = []
                    self._format_where(subwhere, subresults)
                    all_subresults.append(subresults[0])
                if key == "$or":
                    result.append(f"({' OR '.join(all_subresults)})")
                elif key == "$and":
                    result.append(f"({' AND '.join(all_subresults)})")
                else:
                    raise ValueError(f"Operator {key} not supported with a list of where clauses")

    def _format_where_document(self, where_document, results):
        operator = list(where_document.keys())[0]
        if operator == "$contains":
            results.append(f"position(document, '{where_document[operator]}') > 0")
        elif operator == "$and" or operator == "$or":
            all_subresults = []
            for subwhere in where_document[operator]:
                subresults = []
                self._format_where_document(subwhere, subresults)
                all_subresults.append(subresults[0])
            if operator == "$or":
                results.append(f"({' OR '.join(all_subresults)})")
            if operator == "$and":
                results.append(f"({' AND '.join(all_subresults)})")
        else:
            raise ValueError(f"Operator {operator} not supported")

    def get(
        self,
        where={},
        collection_name=None,
        collection_uuid=None,
        ids=None,
        sort=None,
        limit=None,
        offset=None,
        where_document={},
    ):
        if collection_name == None and collection_uuid == None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        s3 = time.time()

        where = self._create_where_clause(
            collection_uuid, ids=ids, where=where, where_document=where_document
        )

        if sort is not None:
            where += f" ORDER BY {sort}"
        else:
            where += f" ORDER BY collection_uuid"  # stable ordering

        if limit is not None or isinstance(limit, int):
            where += f" LIMIT {limit}"

        if offset is not None or isinstance(offset, int):
            where += f" OFFSET {offset}"

        val = self._get(where=where)

        return val

    def _count(self, collection_uuid):
        where_string = ""
        if collection_uuid is not None:
            where_string = f"WHERE collection_uuid = '{collection_uuid}'"
        return self._get_conn().query(f"SELECT COUNT() FROM embeddings {where_string}").result_rows

    def count(self, collection_name):
        collection_uuid = self.get_collection_uuid_from_name(collection_name)
        return self._count(collection_uuid=collection_uuid)[0][0]

    def _delete(self, where_str=None):
        deleted_uuids = (
            self._get_conn().query(f"""SELECT uuid FROM embeddings {where_str}""").result_rows
        )
        self._get_conn().command(
            f"""
            DELETE FROM
                embeddings
        {where_str}
        """
        )
        return [res[0] for res in deleted_uuids] if len(deleted_uuids) > 0 else []

    def delete(
        self, where={}, collection_name=None, collection_uuid=None, ids=None, where_document={}
    ):
        if collection_name == None and collection_uuid == None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        s3 = time.time()
        where_str = self._create_where_clause(
            collection_uuid, ids=ids, where=where, where_document=where_document
        )

        deleted_uuids = self._delete(where_str)

        # if len(where) == 1:
        #     self._idx.delete(collection_uuid)
        self._idx.delete_from_index(collection_uuid, deleted_uuids)

        return deleted_uuids

    def get_by_ids(self, ids: list):
        return (
            self._get_conn()
            .query(
                f"""
        SELECT {db_schema_to_keys()} FROM embeddings WHERE uuid IN ({[id.hex for id in ids]})
        """
            )
            .result_rows
        )

    def get_nearest_neighbors(
        self,
        where: Where,
        where_document: WhereDocument,
        embeddings: Embeddings,
        n_results: int,
        collection_name=None,
        collection_uuid=None,
    ) -> Tuple[List[List[uuid.UUID]], List[List[float]]]:

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        if len(where) != 0 or len(where_document) != 0:
            results = self.get(
                collection_uuid=collection_uuid, where=where, where_document=where_document
            )

            if len(results) > 0:
                ids = [x[1] for x in results]
            else:
                raise NoDatapointsException("No datapoints found for the supplied filter")
        else:
            ids = None
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

    def add_incremental(self, collection_uuid, uuids, embeddings):
        self._idx.add_incremental(collection_uuid, uuids, embeddings)

    def has_index(self, collection_uuid: str):
        return self._idx.has_index(collection_uuid)

    def reset(self):
        conn = self._get_conn()
        conn.command("DROP TABLE collections")
        conn.command("DROP TABLE embeddings")
        self._create_table_collections(conn)
        self._create_table_embeddings(conn)

        self._idx.reset()
        self._idx = Hnswlib(self._settings)

    def raw_sql(self, sql):
        return self._get_conn().query(sql).result_rows
