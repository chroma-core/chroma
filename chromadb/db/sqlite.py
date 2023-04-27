from chromadb.api.types import Documents, Embeddings, IDs, Metadatas, Where, WhereDocument
from chromadb.db.index.hnswlib import Hnswlib, delete_all_indexes
from chromadb.db import DB
from chromadb.errors import (
    NoDatapointsException,
)

from typing import List, Optional, Sequence, Dict, Tuple, cast
import pandas as pd
import json
import numpy.typing as npt
import sqlite3
import uuid
import os
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


def clickhouse_to_sqlite_schema(table_schema):
    for item in table_schema:
        if "embedding" in item:
            item["embedding"] = "TEXT"
        # capitalize the key
        item[list(item.keys())[0]] = item[list(item.keys())[0]].upper()
        if "NULLABLE" in item[list(item.keys())[0]]:
            item[list(item.keys())[0]] = "TEXT"
        if "UUID" in item[list(item.keys())[0]]:
            item[list(item.keys())[0]] = "TEXT"
        if "FLOAT64" in item[list(item.keys())[0]]:
            item[list(item.keys())[0]] = "REAL"
        if "STRING" in item[list(item.keys())[0]]:
            item[list(item.keys())[0]] = "TEXT"
        if "ARRAY" in item[list(item.keys())[0]]:
            item[list(item.keys())[0]] = "TEXT"

    return table_schema


class SQLite(DB):
    index_cache = {}

    def __init__(self, settings):
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._create_table_collections()
        self._create_table_embeddings()
        self._settings = settings

    def commit(self):
        self._conn.commit()

    def _create_table_collections(self):
        self._conn.execute(
            """CREATE TABLE collections (uuid text , name text, metadata text) """
        )
        self.commit()

    # SQLite has different types, so we want to convert the clickhouse schema to sqlite schema
    def _create_table_embeddings(self):
        self._conn.execute(
            """CREATE TABLE embeddings (
            collection_uuid text, uuid text, embedding text, document text, id text, metadata text
        ) """
        )
        self.commit()

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

    def get_collection_uuid_from_name(self, name):
        return self._conn.execute(
            "SELECT uuid FROM collections WHERE name = ?", (name,)
        ).fetchall()[0][0]

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
            if len(ids) == 1:
                where_clauses.append(f" id = '{ids[0]}'")
            else:
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
            if get_or_create is True:
                logger.info(
                    f"collection with name {name} already exists, returning existing collection"
                )
                return dupe_check
            else:
                raise ValueError(f"Collection with name {name} already exists")

        collection_uuid = uuid.uuid4()
        self._conn.execute(
            """INSERT INTO collections (uuid, name, metadata) VALUES (?, ?, ?)""",
            [str(collection_uuid), name, json.dumps(metadata)],
        )
        self.commit()
        return [[str(collection_uuid), name, metadata]]

    def get_collection(self, name: str) -> Sequence:
        res = self._conn.execute("""SELECT * FROM collections WHERE name = ?""", (name,)).fetchall()
        # json.loads the metadata
        return [[x[0], x[1], json.loads(x[2])] for x in res]

    def get_collection_by_id(self, uuid: str) -> Sequence:
        res = self._conn.execute("""SELECT * FROM collections WHERE uuid = ?""", (uuid,)).fetchone()
        return [res[0], res[1], json.loads(res[2])]

    def list_collections(self) -> Sequence:
        res = self._conn.execute("""SELECT * FROM collections""").fetchall()
        return [[x[0], x[1], json.loads(x[2])] for x in res]

    def delete_collection(self, name: str):
        collection_uuid = self.get_collection_uuid_from_name(name)
        self._conn.execute(
            """DELETE FROM embeddings WHERE collection_uuid = ?""", (collection_uuid,)
        )

        self._delete_index(collection_uuid)
        self._conn.execute("""DELETE FROM collections WHERE name = ?""", (name,))
        self.commit()

    def update_collection(
        self, current_name: str, new_name: str, new_metadata: Optional[Dict] = None
    ):
        if new_name is None:
            new_name = current_name
        if new_metadata is None:
            new_metadata = self.get_collection(current_name)[0][2]

        self._conn.execute(
            """UPDATE collections SET name = ?, metadata = ? WHERE name = ?""",
            (
                new_name,
                json.dumps(new_metadata),
                current_name,
            ),
        )
        self.commit()

    #
    #  ITEM METHODS
    #
    # the execute many syntax is different than clickhouse, the (?,?) syntax is different than clickhouse
    def add(self, collection_uuid, embeddings, metadatas, documents, ids):
        data_to_insert = [
            [
                collection_uuid,
                str(uuid.uuid4()),
                json.dumps(embedding),
                json.dumps(metadatas[i]) if metadatas else None,
                documents[i] if documents else None,
                ids[i],
            ]
            for i, embedding in enumerate(embeddings)
        ]
        # json.dumps the metadata and embedding

        insert_string = "collection_uuid, uuid, embedding, metadata, document, id"

        self._conn.executemany(
            f"""
         INSERT INTO embeddings ({insert_string}) VALUES (?,?,?,?,?,?)""",
            data_to_insert,
        )
        self.commit()

        return [uuid.UUID(x[1]) for x in data_to_insert]  # return uuids

    def _count(self, collection_uuid):
        where_string = f"WHERE collection_uuid = '{collection_uuid}'"
        return self._conn.execute(f"SELECT COUNT() FROM embeddings {where_string}")

    def count(self, collection_name=None):
        collection_uuid = self.get_collection_uuid_from_name(collection_name)
        return self._count(collection_uuid=collection_uuid).fetchall()[0][0]

# TODO

    def _format_where(self, where, result):
        for key, value in where.items():
            # Shortcut for $eq
            if type(value) == str:
                result.append(f" CAST(json_extract(metadata,'$.{key}') AS TEXT) = '{value}'")
            if type(value) == int:
                result.append(f" CAST(json_extract(metadata,'$.{key}') AS INT) = {value}")
            if type(value) == float:
                result.append(f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) = {value}")
            # Operator expression
            elif type(value) == dict:
                operator, operand = list(value.items())[0]
                if operator == "$gt":
                    result.append(f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) > {operand}")
                elif operator == "$lt":
                    result.append(f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) < {operand}")
                elif operator == "$gte":
                    result.append(f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) >= {operand}")
                elif operator == "$lte":
                    result.append(f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) <= {operand}")
                elif operator == "$ne":
                    if type(operand) == str:
                        return result.append(
                            f" CAST(json_extract(metadata,'$.{key}') AS TEXT) != '{operand}'"
                        )
                    return result.append(
                        f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) != {operand}"
                    )
                elif operator == "$eq":
                    if type(operand) == str:
                        return result.append(
                            f" CAST(json_extract(metadata,'$.{key}') AS TEXT) = '{operand}'"
                        )
                    return result.append(
                        f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) = {operand}"
                    )
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
            results.append(f"INSTR(document, '{where_document[operator]}') > 0")
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

    def _get(self, where, columns: Optional[List] = None):
        select_columns = db_schema_to_keys() if columns is None else columns
        val = self._conn.execute(
            f"""SELECT {",".join(select_columns)} FROM embeddings {where}"""
        ).fetchall()
        for i in range(len(val)):
            val[i] = list(val[i])
            if "collection_uuid" in select_columns:
                collection_uuid_column_index = select_columns.index("collection_uuid")
                val[i][collection_uuid_column_index] = uuid.UUID(
                    val[i][collection_uuid_column_index]
                )
            if "uuid" in select_columns:
                uuid_column_index = select_columns.index("uuid")
                val[i][uuid_column_index] = uuid.UUID(val[i][uuid_column_index])
            # json.loads the metadata
            if "metadata" in select_columns:
                metadata_column_index = select_columns.index("metadata")
                val[i][metadata_column_index] = (
                    json.loads(val[i][metadata_column_index])
                    if val[i][metadata_column_index]
                    else None
                )
            # json.loads the embedding
            if "embedding" in select_columns:
                metadata_column_index = select_columns.index("embedding")
                val[i][metadata_column_index] = (
                    json.loads(val[i][metadata_column_index])
                    if val[i][metadata_column_index]
                    else None
                )

        return val

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

    def _update(
        self,
        collection_uuid,
        ids: IDs,
        embeddings: Optional[Embeddings],
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
    ):
        update_data = []
        for i in range(len(ids)):
            data = []
            update_data.append(data)
            if embeddings is not None:
                data.append(json.dumps(embeddings[i]))
            if metadatas is not None:
                data.append(json.dumps(metadatas[i]))
            if documents is not None:
                data.append(documents[i])
            data.append(ids[i])

        update_fields = []
        if embeddings is not None:
            update_fields.append("embedding = ?")
        if metadatas is not None:
            update_fields.append("metadata = ?")
        if documents is not None:
            update_fields.append("document = ?")

        update_statement = f"""
        UPDATE
            embeddings
        SET
            {", ".join(update_fields)}
        WHERE
            id = ? AND
            collection_uuid = '{collection_uuid}';
        """
        self._conn.executemany(update_statement, update_data)
        self.commit()

    def _delete(self, where_str: Optional[str] = None) -> List:
        uuids_deleted = self._conn.execute(
            f"""SELECT uuid FROM embeddings {where_str}"""
        ).fetchall()
        self._conn.execute(
            f"""DELETE FROM embeddings {where_str}"""
        )
        self.commit()
        return [uuid.UUID(x[0]) for x in uuids_deleted]

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

    def get_by_ids(self, ids: List, columns: Optional[List] = None):
        # select from SQLite DB table where ids are in the list
        if not isinstance(ids, list):
            raise TypeError(f"Expected ids to be a list, got {ids}")

        if not ids:
            # create an empty pandas dataframe
            return pd.DataFrame()

        columns = columns + ["uuid"] if columns else ["uuid"]

        select_columns = db_schema_to_keys() if columns is None else columns
        resp = self._conn.execute(
            f"""
            SELECT
                {",".join(select_columns)}
            FROM
                embeddings
            WHERE
                uuid IN ({','.join([("'" + str(x) + "'") for x in ids])})
        """
        ).fetchall()
        if "embedding" in select_columns:
            response = tuple(tuple(json.loads(item) if i == select_columns.index("embedding") else item for i, item in enumerate(t)) for t in resp)
        else:
            response = resp
        # sort db results by the order of the uuids
        response = sorted(response, key=lambda obj: ids.index(uuid.UUID(obj[len(columns) - 1])))

        return response

    def raw_sql(self, sql):
        return self._conn.execute(sql).df()

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
        self._conn.execute("DROP TABLE collections")
        self._conn.execute("DROP TABLE embeddings")
        self._create_table_collections()
        self._create_table_embeddings()
        self.commit()
        self.reset_indexes()

    def __del__(self):
        logger.info("Exiting: Cleaning up .chroma directory")
        self.reset_indexes()

    def persist(self):
        raise NotImplementedError(
            "Set chroma_db_impl='sqlite+persist' to get persistence functionality"
        )


class PersistentSQLite(SQLite):
    _save_folder = None

    def __init__(self, settings):
        super().__init__(settings=settings)

        if settings.persist_directory == ".chroma":
            raise ValueError(
                "You cannot use chroma's cache directory .chroma/, please set a different directory"
            )

        self._save_folder = settings.persist_directory
        self.load()

    def set_save_folder(self, path):
        self._save_folder = path

    def get_save_folder(self):
        return self._save_folder

    def persist(self):
        """
        Persist the database to disk
        """
        logger.info(f"Persisting DB to disk, putting it in the save folder: {self._save_folder}")
        if self._conn is None:
            return

        if not os.path.exists(self._save_folder):
            os.makedirs(self._save_folder)

        # if the db is empty, dont save
        if self._conn.execute("SELECT COUNT() FROM embeddings") == 0:
            return

        backup_db = sqlite3.connect(f'{self._save_folder}/sqlite_backup.db', check_same_thread=False)
        self._conn.backup(backup_db)
        # return
        # backup_db.close()

    def load(self):
        """
        Load the database from disk
        """
        if not os.path.exists(self._save_folder):
            os.makedirs(self._save_folder)

        # load in the db
        if not os.path.exists(f"{self._save_folder}/sqlite_backup.db"):
            logger.info(f"No existing DB found in {self._save_folder}, skipping load")
        else:
            backup_db = sqlite3.connect(f'{self._save_folder}/sqlite_backup.db')
            backup_db.backup(self._conn)
            logger.info(
                f"""loaded in {self._conn.execute(f"SELECT COUNT() FROM embeddings").fetchall()[0][0]} embeddings"""
            )
            backup_db.close()

    def __del__(self):
        logger.info("PersistentSQLite del, about to run persist")
        self.persist()

    def reset(self):
        super().reset()
        # empty the save folder
        import shutil
        import os

        shutil.rmtree(self._save_folder)
        os.mkdir(self._save_folder)
