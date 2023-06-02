# type: ignore
from chromadb.config import System
from chromadb.api.types import Documents, Embeddings, IDs, Metadatas
from chromadb.db.clickhouse import (
    Clickhouse,
    db_array_schema_to_clickhouse_schema,
    EMBEDDING_TABLE_SCHEMA,
    db_schema_to_keys,
    COLLECTION_TABLE_SCHEMA,
)
from typing import List, Optional, Sequence
import pandas as pd
import json
import duckdb
import uuid
import os
import logging
import atexit
from uuid import UUID
from overrides import override
from chromadb.api.types import Metadata

logger = logging.getLogger(__name__)


def clickhouse_to_duckdb_schema(table_schema):
    for item in table_schema:
        if "embedding" in item:
            item["embedding"] = "DOUBLE[]"
        # capitalize the key
        item[list(item.keys())[0]] = item[list(item.keys())[0]].upper()
        if "NULLABLE" in item[list(item.keys())[0]]:
            item[list(item.keys())[0]] = (
                item[list(item.keys())[0]].replace("NULLABLE(", "").replace(")", "")
            )
        if "UUID" in item[list(item.keys())[0]]:
            item[list(item.keys())[0]] = "STRING"
        if "FLOAT64" in item[list(item.keys())[0]]:
            item[list(item.keys())[0]] = "DOUBLE"
    return table_schema


# TODO: inherits ClickHouse for convenience of copying behavior, not
# because it's logically a subtype. Factoring out the common behavior
# to a third superclass they both extend would be preferable.
class DuckDB(Clickhouse):
    # duckdb has a different way of connecting to the database
    def __init__(self, system: System):
        self._conn = duckdb.connect()
        self._create_table_collections(self._conn)
        self._create_table_embeddings(self._conn)
        self._settings = system.settings

        # Normally this would be handled by super(), but we actually can't invoke
        # super().__init__ here because we're (incorrectly) inheriting from Clickhouse
        self._dependencies = set()

        # https://duckdb.org/docs/extensions/overview
        self._conn.execute("LOAD 'json';")

    @override
    def _create_table_collections(self, conn):
        conn.execute(
            f"""CREATE TABLE collections (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(COLLECTION_TABLE_SCHEMA))}
        ) """
        )

    # duckdb has different types, so we want to convert the clickhouse schema to duckdb schema
    @override
    def _create_table_embeddings(self, conn):
        conn.execute(
            f"""CREATE TABLE embeddings (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(EMBEDDING_TABLE_SCHEMA))}
        ) """
        )

    #
    #  UTILITY METHODS
    #
    @override
    def get_collection_uuid_from_name(self, collection_name: str) -> UUID:
        return self._conn.execute(
            "SELECT uuid FROM collections WHERE name = ?", [collection_name]
        ).fetchall()[0][0]

    #
    #  COLLECTION METHODS
    #
    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[Metadata] = None,
        get_or_create: bool = False,
    ) -> Sequence:
        # poor man's unique constraint
        dupe_check = self.get_collection(name)
        if len(dupe_check) > 0:
            if get_or_create is True:
                if dupe_check[0][2] != metadata:
                    self.update_collection(
                        dupe_check[0][0], new_name=name, new_metadata=metadata
                    )
                    dupe_check = self.get_collection(name)

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
        return [[str(collection_uuid), name, metadata]]

    @override
    def get_collection(self, name: str) -> Sequence:
        res = self._conn.execute(
            """SELECT * FROM collections WHERE name = ?""", [name]
        ).fetchall()
        # json.loads the metadata
        return [[x[0], x[1], json.loads(x[2])] for x in res]

    @override
    def get_collection_by_id(self, collection_uuid: str):
        res = self._conn.execute(
            """SELECT * FROM collections WHERE uuid = ?""", [collection_uuid]
        ).fetchone()
        return [res[0], res[1], json.loads(res[2])]

    @override
    def list_collections(self) -> Sequence:
        res = self._conn.execute("""SELECT * FROM collections""").fetchall()
        return [[x[0], x[1], json.loads(x[2])] for x in res]

    @override
    def delete_collection(self, name: str):
        collection_uuid = self.get_collection_uuid_from_name(name)
        self._conn.execute(
            """DELETE FROM embeddings WHERE collection_uuid = ?""", [collection_uuid]
        )

        self._delete_index(collection_uuid)
        self._conn.execute("""DELETE FROM collections WHERE name = ?""", [name])

    @override
    def update_collection(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[Metadata] = None,
    ):
        if new_name is not None:
            dupe_check = self.get_collection(new_name)
            if len(dupe_check) > 0 and dupe_check[0][0] != str(id):
                raise ValueError(f"Collection with name {new_name} already exists")

            self._conn.execute(
                """UPDATE collections SET name = ? WHERE uuid = ?""",
                [new_name, id],
            )

        if new_metadata is not None:
            self._conn.execute(
                """UPDATE collections SET metadata = ? WHERE uuid = ?""",
                [json.dumps(new_metadata), id],
            )

    #
    #  ITEM METHODS
    #
    # the execute many syntax is different than clickhouse, the (?,?) syntax is different than clickhouse
    @override
    def add(self, collection_uuid, embeddings, metadatas, documents, ids) -> List[UUID]:
        data_to_insert = [
            [
                collection_uuid,
                str(uuid.uuid4()),
                embedding,
                json.dumps(metadatas[i]) if metadatas else None,
                documents[i] if documents else None,
                ids[i],
            ]
            for i, embedding in enumerate(embeddings)
        ]

        insert_string = "collection_uuid, uuid, embedding, metadata, document, id"

        self._conn.executemany(
            f"""
         INSERT INTO embeddings ({insert_string}) VALUES (?,?,?,?,?,?)""",
            data_to_insert,
        )

        return [uuid.UUID(x[1]) for x in data_to_insert]  # return uuids

    @override
    def count(self, collection_id: UUID) -> int:
        where_string = f"WHERE collection_uuid = '{collection_id}'"
        return self._conn.query(
            f"SELECT COUNT() FROM embeddings {where_string}"
        ).fetchall()[0][0]

    @override
    def _format_where(self, where, result):
        for key, value in where.items():
            # Shortcut for $eq
            if type(value) == str:
                result.append(f" json_extract_string(metadata,'$.{key}') = '{value}'")
            if type(value) == int:
                result.append(
                    f" CAST(json_extract(metadata,'$.{key}') AS INT) = {value}"
                )
            if type(value) == float:
                result.append(
                    f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) = {value}"
                )
            # Operator expression
            elif type(value) == dict:
                operator, operand = list(value.items())[0]
                if operator == "$gt":
                    result.append(
                        f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) > {operand}"
                    )
                elif operator == "$lt":
                    result.append(
                        f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) < {operand}"
                    )
                elif operator == "$gte":
                    result.append(
                        f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) >= {operand}"
                    )
                elif operator == "$lte":
                    result.append(
                        f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) <= {operand}"
                    )
                elif operator == "$ne":
                    if type(operand) == str:
                        return result.append(
                            f" json_extract_string(metadata,'$.{key}') != '{operand}'"
                        )
                    return result.append(
                        f" CAST(json_extract(metadata,'$.{key}') AS DOUBLE) != {operand}"
                    )
                elif operator == "$eq":
                    if type(operand) == str:
                        return result.append(
                            f" json_extract_string(metadata,'$.{key}') = '{operand}'"
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
                    raise ValueError(
                        f"Operator {key} not supported with a list of where clauses"
                    )

    @override
    def _format_where_document(self, where_document, results):
        operator = list(where_document.keys())[0]
        if operator == "$contains":
            results.append(f"position('{where_document[operator]}' in document) > 0")
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

    @override
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
            if "metadata" in select_columns:
                metadata_column_index = select_columns.index("metadata")
                val[i][metadata_column_index] = (
                    json.loads(val[i][metadata_column_index])
                    if val[i][metadata_column_index]
                    else None
                )

        return val

    @override
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
                data.append(embeddings[i])
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

    @override
    def _delete(self, where_str: Optional[str] = None) -> List:
        uuids_deleted = self._conn.execute(
            f"""SELECT uuid FROM embeddings {where_str}"""
        ).fetchall()
        self._conn.execute(
            f"""
            DELETE FROM
                embeddings
        {where_str}
        """
        ).fetchall()[0]
        return [uuid.UUID(x[0]) for x in uuids_deleted]

    @override
    def get_by_ids(
        self, uuids: List[UUID], columns: Optional[List[str]] = None
    ) -> Sequence:
        # select from duckdb table where ids are in the list
        if not isinstance(uuids, list):
            raise TypeError(f"Expected ids to be a list, got {uuids}")

        if not uuids:
            # create an empty pandas dataframe
            return pd.DataFrame()

        columns = columns + ["uuid"] if columns else ["uuid"]

        select_columns = db_schema_to_keys() if columns is None else columns
        response = self._conn.execute(
            f"""
            SELECT
                {",".join(select_columns)}
            FROM
                embeddings
            WHERE
                uuid IN ({','.join([("'" + str(x) + "'") for x in uuids])})
        """
        ).fetchall()

        # sort db results by the order of the uuids
        response = sorted(
            response, key=lambda obj: uuids.index(uuid.UUID(obj[len(columns) - 1]))
        )

        return response

    @override
    def raw_sql(self, raw_sql):
        return self._conn.execute(raw_sql).df()

    # TODO: This method should share logic with clickhouse impl
    @override
    def reset_state(self):
        self._conn.execute("DROP TABLE collections")
        self._conn.execute("DROP TABLE embeddings")
        self._create_table_collections(self._conn)
        self._create_table_embeddings(self._conn)

        self.reset_indexes()

    def __del__(self):
        logger.info("Exiting: Cleaning up .chroma directory")
        self.reset_indexes()

    @override
    def persist(self) -> None:
        raise NotImplementedError(
            "Set chroma_db_impl='duckdb+parquet' to get persistence functionality"
        )


class PersistentDuckDB(DuckDB):
    _save_folder = None

    def __init__(self, system: System):
        super().__init__(system=system)

        system.settings.require("persist_directory")

        if system.settings.persist_directory == ".chroma":
            raise ValueError(
                "You cannot use chroma's cache directory .chroma/, please set a different directory"
            )

        self._save_folder = system.settings.persist_directory
        self.load()
        # https://docs.python.org/3/library/atexit.html
        atexit.register(self.persist)

    def set_save_folder(self, path):
        self._save_folder = path

    def get_save_folder(self):
        return self._save_folder

    @override
    def persist(self):
        """
        Persist the database to disk
        """
        logger.info(
            f"Persisting DB to disk, putting it in the save folder: {self._save_folder}"
        )
        if self._conn is None:
            return

        if not os.path.exists(self._save_folder):
            os.makedirs(self._save_folder)

        # if the db is empty, dont save
        if self._conn.query("SELECT COUNT() FROM embeddings") == 0:
            return

        self._conn.execute(
            f"""
            COPY
                (SELECT * FROM embeddings)
            TO '{self._save_folder}/chroma-embeddings.parquet'
                (FORMAT PARQUET);
        """
        )

        self._conn.execute(
            f"""
            COPY
                (SELECT * FROM collections)
            TO '{self._save_folder}/chroma-collections.parquet'
                (FORMAT PARQUET);
        """
        )

    def load(self):
        """
        Load the database from disk
        """
        if not os.path.exists(self._save_folder):
            os.makedirs(self._save_folder)

        # load in the embeddings
        if not os.path.exists(f"{self._save_folder}/chroma-embeddings.parquet"):
            logger.info(f"No existing DB found in {self._save_folder}, skipping load")
        else:
            path = self._save_folder + "/chroma-embeddings.parquet"
            self._conn.execute(
                f"INSERT INTO embeddings SELECT * FROM read_parquet('{path}');"
            )
            logger.info(
                f"""loaded in {self._conn.query(f"SELECT COUNT() FROM embeddings").fetchall()[0][0]} embeddings"""
            )

        # load in the collections
        if not os.path.exists(f"{self._save_folder}/chroma-collections.parquet"):
            logger.info(f"No existing DB found in {self._save_folder}, skipping load")
        else:
            path = self._save_folder + "/chroma-collections.parquet"
            self._conn.execute(
                f"INSERT INTO collections SELECT * FROM read_parquet('{path}');"
            )
            logger.info(
                f"""loaded in {self._conn.query(f"SELECT COUNT() FROM collections").fetchall()[0][0]} collections"""
            )

    def __del__(self):
        # No-op for duckdb with persistence since the base class will delete the indexes
        pass

    @override
    def reset_state(self):
        super().reset_state()
        # empty the save folder
        import shutil
        import os

        shutil.rmtree(self._save_folder)
        os.mkdir(self._save_folder)
