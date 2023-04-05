from chromadb.api.types import Documents, Embeddings, IDs, Metadatas, Where, WhereDocument
from chromadb.db import DB
from chromadb.db.index.hnswlib import Hnswlib, delete_all_indexes
from chromadb.errors import (
    NoDatapointsException,
)
import uuid
import numpy.typing as npt
import json
from typing import Dict, Optional, Sequence, List, Tuple, cast
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect import common
import logging

logger = logging.getLogger(__name__)

COLLECTION_TABLE_SCHEMA = [{"uuid": "UUID"}, {"name": "String"}, {"metadata": "String"}]

EMBEDDING_TABLE_SCHEMA = [
    {"collection_uuid": "UUID"},
    {"uuid": "UUID"},
    {"embedding": "Array(Float64)"},
    {"document": "Nullable(String)"},
    {"id": "Nullable(String)"},
    {"metadata": "Nullable(String)"},
]


def db_array_schema_to_clickhouse_schema(table_schema):
    return_str = ""
    for element in table_schema:
        for k, v in element.items():
            return_str += f"{k} {v}, "
    return return_str


def db_schema_to_keys() -> List[str]:
    keys = []
    for element in EMBEDDING_TABLE_SCHEMA:
        keys.append(list(element.keys())[0])
    return keys


class Clickhouse(DB):
    #
    #  INIT METHODS
    #
    def __init__(self, settings):
        self._conn = None
        self._settings = settings

    def _init_conn(self):
        common.set_setting("autogenerate_session_id", False)
        self._conn = clickhouse_connect.get_client(
            host=self._settings.clickhouse_host, port=int(self._settings.clickhouse_port)
        )
        self._create_table_collections(self._conn)
        self._create_table_embeddings(self._conn)

    def _get_conn(self) -> Client:
        if self._conn is None:
            self._init_conn()
        return self._conn  # type: ignore because we know it's not None

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

    index_cache = {}

    def _index(self, collection_id):
        """Retrieve an HNSW index instance for the given collection"""

        if collection_id not in self.index_cache:
            coll = self.get_collection_by_id(collection_id)
            collection_metadata = coll[2]
            index = Hnswlib(collection_id, self._settings, collection_metadata)
            self.index_cache[collection_id] = index

        return self.index_cache[collection_id]

    def _delete_index(self, collection_id):
        """Delete an index from the cache"""
        index = self._index(collection_id)
        index.delete()
        del self.index_cache[collection_id]

    #
    #  UTILITY METHODS
    #
    def persist(self):
        raise NotImplementedError("Clickhouse is a persistent database, this method is not needed")

    def get_collection_uuid_from_name(self, name: str) -> str:
        res = self._get_conn().query(
            f"""
            SELECT uuid FROM collections WHERE name = '{name}'
        """
        )
        return res.result_rows[0][0]

    def _create_where_clause(
        self,
        collection_uuid: str,
        ids: Optional[List[str]] = None,
        where: Where = {},
        where_document: WhereDocument = {},
    ):
        where_clauses: List[str] = []
        self._format_where(where, where_clauses)
        if len(where_document) > 0:
            where_document_clauses = []
            self._format_where_document(where_document, where_document_clauses)
            where_clauses.extend(where_document_clauses)

        if ids is not None:
            where_clauses.append(f" id IN {tuple(ids)}")

        where_clauses.append(f"collection_uuid = '{collection_uuid}'")
        where_str = " AND ".join(where_clauses)
        where_str = f"WHERE {where_str}"
        return where_str

    #
    #  COLLECTION METHODS
    #
    def create_collection(
        self, name: str, metadata: Optional[Dict] = None, get_or_create: bool = False
    ) -> Sequence:
        # poor man's unique constraint
        dupe_check = self.get_collection(name)

        if len(dupe_check) > 0:
            if get_or_create:
                logger.info(
                    f"collection with name {name} already exists, returning existing collection"
                )
                return dupe_check
            else:
                raise ValueError(f"Collection with name {name} already exists")

        collection_uuid = uuid.uuid4()
        data_to_insert = [[collection_uuid, name, json.dumps(metadata)]]

        self._get_conn().insert(
            "collections", data_to_insert, column_names=["uuid", "name", "metadata"]
        )
        return [[collection_uuid, name, metadata]]

    def get_collection(self, name: str):
        res = (
            self._get_conn()
            .query(
                f"""
         SELECT * FROM collections WHERE name = '{name}'
         """
            )
            .result_rows
        )
        # json.loads the metadata
        return [[x[0], x[1], json.loads(x[2])] for x in res]

    def get_collection_by_id(self, collection_uuid: str):
        res = (
            self._get_conn()
            .query(
                f"""
         SELECT * FROM collections WHERE uuid = '{collection_uuid}'
         """
            )
            .result_rows
        )
        # json.loads the metadata
        return [[x[0], x[1], json.loads(x[2])] for x in res][0]

    def list_collections(self) -> Sequence:
        res = self._get_conn().query("SELECT * FROM collections").result_rows
        return [[x[0], x[1], json.loads(x[2])] for x in res]

    def update_collection(
        self, current_name: str, new_name: Optional[str] = None, new_metadata: Optional[Dict] = None
    ):
        if new_name is None:
            new_name = current_name
        if new_metadata is None:
            new_metadata = self.get_collection(current_name)[0][2]

        return self._get_conn().command(
            f"""

         ALTER TABLE
            collections
         UPDATE
            metadata = '{json.dumps(new_metadata)}',
            name = '{new_name}'
         WHERE
            name = '{current_name}'
         """
        )

    def delete_collection(self, name: str):
        collection_uuid = self.get_collection_uuid_from_name(name)
        self._get_conn().command(
            f"""
        DELETE FROM embeddings WHERE collection_uuid = '{collection_uuid}'
        """
        )

        self._delete_index(collection_uuid)

        self._get_conn().command(
            f"""
         DELETE FROM collections WHERE name = '{name}'
         """
        )

    #
    #  ITEM METHODS
    #

    def add(self, collection_uuid, embeddings, metadatas, documents, ids):
        data_to_insert = [
            [
                collection_uuid,
                uuid.uuid4(),
                embedding,
                json.dumps(metadatas[i]) if metadatas else None,
                documents[i] if documents else None,
                ids[i],
            ]
            for i, embedding in enumerate(embeddings)
        ]
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
            raise ValueError(f"Could not find {len(ids) - len(existing_items)} items for update")

        # Update the db
        self._update(collection_uuid, ids, embeddings, metadatas, documents)

        # Update the index
        if embeddings is not None:
            update_uuids = [x[1] for x in existing_items]
            index = self._index(collection_uuid)
            index.add(update_uuids, embeddings, update=True)

    def _get(self, where={}, columns: Optional[List] = None):
        select_columns = db_schema_to_keys() if columns is None else columns
        val = (
            self._get_conn()
            .query(f"""SELECT {",".join(select_columns)} FROM embeddings {where}""")
            .result_rows
        )
        for i in range(len(val)):
            # We know val has index abilities, so cast it for typechecker
            val = cast(list, val)
            val[i] = list(val[i])
            # json.load the metadata
            if "metadata" in select_columns:
                metadata_column_index = select_columns.index("metadata")
                db_metadata = val[i][metadata_column_index]
                val[i][metadata_column_index] = json.loads(db_metadata) if db_metadata else None
        return val

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
                    raise ValueError(
                        f"Expected one of $gt, $lt, $gte, $lte, $ne, $eq, got {operator}"
                    )
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
                    raise ValueError(f"Expected one of $or, $and, got {key}")

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
            raise ValueError(f"Expected one of $contains, $and, $or, got {operator}")

    def get(
        self,
        where: Where = {},
        collection_name: Optional[str] = None,
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: WhereDocument = {},
        columns: Optional[List[str]] = None,
    ) -> Sequence:
        if collection_name is None and collection_uuid is None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        where_str = self._create_where_clause(
            # collection_uuid must be defined at this point, cast it for typechecker
            cast(str, collection_uuid),
            ids=ids,
            where=where,
            where_document=where_document,
        )

        if sort is not None:
            where_str += f" ORDER BY {sort}"
        else:
            where_str += " ORDER BY collection_uuid"  # stable ordering

        if limit is not None or isinstance(limit, int):
            where_str += f" LIMIT {limit}"

        if offset is not None or isinstance(offset, int):
            where_str += f" OFFSET {offset}"

        val = self._get(where=where_str, columns=columns)

        return val

    def _count(self, collection_uuid: str):
        where_string = f"WHERE collection_uuid = '{collection_uuid}'"
        return self._get_conn().query(f"SELECT COUNT() FROM embeddings {where_string}").result_rows

    def count(self, collection_name: str):
        collection_uuid = self.get_collection_uuid_from_name(collection_name)
        return self._count(collection_uuid=collection_uuid)[0][0]

    def _delete(self, where_str: Optional[str] = None) -> List:
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
        self,
        where: Where = {},
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        where_document: WhereDocument = {},
    ) -> List:
        where_str = self._create_where_clause(
            # collection_uuid must be defined at this point, cast it for typechecker
            cast(str, collection_uuid),
            ids=ids,
            where=where,
            where_document=where_document,
        )

        deleted_uuids = self._delete(where_str)

        index = self._index(collection_uuid)
        index.delete_from_index(deleted_uuids)

        return deleted_uuids

    def get_by_ids(self, ids: list, columns: Optional[List] = None):
        columns = columns + ["uuid"] if columns else ["uuid"]
        select_columns = db_schema_to_keys() if columns is None else columns
        response = (
            self._get_conn()
            .query(
                f"""
        SELECT {",".join(select_columns)} FROM embeddings WHERE uuid IN ({[id.hex for id in ids]})
        """
            )
            .result_rows
        )

        # sort db results by the order of the uuids
        response = sorted(response, key=lambda obj: ids.index(obj[len(columns) - 1]))

        return response

    def get_nearest_neighbors(
        self,
        where: Where,
        where_document: WhereDocument,
        embeddings: Embeddings,
        n_results: int,
        collection_name=None,
        collection_uuid=None,
    ) -> Tuple[List[List[uuid.UUID]], npt.NDArray]:

        # Either the collection name or the collection uuid must be provided
        if collection_name is None and collection_uuid is None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        if len(where) != 0 or len(where_document) != 0:
            results = self.get(
                collection_uuid=collection_uuid, where=where, where_document=where_document
            )

            if len(results) > 0:
                ids = [x[1] for x in results]
            else:
                raise NoDatapointsException(
                    f"No datapoints found for the supplied filter {json.dumps(where)}"
                )
        else:
            ids = None

        index = self._index(collection_uuid)
        uuids, distances = index.get_nearest_neighbors(embeddings, n_results, ids)

        return uuids, distances

    def create_index(self, collection_uuid: str):
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

        index = self._index(collection_uuid)
        index.add(uuids, embeddings)

    def add_incremental(self, collection_uuid, uuids, embeddings):
        index = self._index(collection_uuid)
        index.add(uuids, embeddings)

    def reset_indexes(self):
        delete_all_indexes(self._settings)
        self.index_cache = {}

    def reset(self):
        conn = self._get_conn()
        conn.command("DROP TABLE collections")
        conn.command("DROP TABLE embeddings")
        self._create_table_collections(conn)
        self._create_table_embeddings(conn)

        self.reset_indexes()

    def raw_sql(self, sql):
        return self._get_conn().query(sql).result_rows
