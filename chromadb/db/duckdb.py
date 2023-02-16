from chromadb.api.types import Documents, Embeddings, IDs, Metadatas
from chromadb.db import DB
from chromadb.db.index.hnswlib import Hnswlib
from chromadb.db.clickhouse import (
    Clickhouse,
    db_array_schema_to_clickhouse_schema,
    EMBEDDING_TABLE_SCHEMA,
    db_schema_to_keys,
    COLLECTION_TABLE_SCHEMA,
)
from typing import Optional, Sequence
import pandas as pd
import json
import duckdb
import uuid
import time
import itertools


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
        self._conn.execute(
            f"""CREATE TABLE collections (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(COLLECTION_TABLE_SCHEMA))}
        ) """
        )

    # duckdb has different types, so we want to convert the clickhouse schema to duckdb schema
    def _create_table_embeddings(self):
        self._conn.execute(
            f"""CREATE TABLE embeddings (
            {db_array_schema_to_clickhouse_schema(clickhouse_to_duckdb_schema(EMBEDDING_TABLE_SCHEMA))}
        ) """
        )

    #
    #  UTILITY METHODS
    #
    def get_collection_uuid_from_name(self, name):
        return self._conn.execute(
            f"""SELECT uuid FROM collections WHERE name = ?""", [name]
        ).fetchall()[0][0]

    #
    #  COLLECTION METHODS
    #
    def create_collection(self, name, metadata=None):
        if metadata is None:
            metadata = {}

        # poor man's unique constraint
        if not self.get_collection(name).empty:
            raise Exception(f"collection with name {name} already exists")

        return self._conn.execute(
            f"""INSERT INTO collections (uuid, name, metadata) VALUES (?, ?, ?)""",
            [str(uuid.uuid4()), name, json.dumps(metadata)],
        )

    def get_collection(self, name):
        return self._conn.execute(f"""SELECT * FROM collections WHERE name = ?""", [name]).df()

    def list_collections(self) -> Sequence[Sequence[str]]:
        return self._conn.execute(f"""SELECT * FROM collections""").fetchall()

    def delete_collection(self, name):
        collection_uuid = self.get_collection_uuid_from_name(name)
        self._conn.execute(
            f"""DELETE FROM embeddings WHERE collection_uuid = ?""", [collection_uuid]
        )
        self._idx.delete_index(collection_uuid)
        self._conn.execute(f"""DELETE FROM collections WHERE name = ?""", [name])
        return True

    def update_collection(self, current_name, new_name, new_metadata):
        if new_name is None:
            new_name = current_name
        if new_metadata is None:
            new_metadata = self.get_collection(current_name).metadata[0]

        self._conn.execute(
            f"""UPDATE collections SET name = ?, metadata = ? WHERE name = ?""",
            [new_name, json.dumps(new_metadata), current_name],
        )

    #
    #  ITEM METHODS
    #
    # the execute many syntax is different than clickhouse, the (?,?) syntax is different than clickhouse
    def add(self, collection_uuid, embedding, metadata, documents, ids):

        data_to_insert = []
        for i in range(len(embedding)):
            data_to_insert.append(
                [
                    collection_uuid,
                    str(uuid.uuid4()),
                    embedding[i],
                    json.dumps(metadata[i]),
                    documents[i],
                    ids[i],
                ]
            )

        insert_string = "collection_uuid, uuid, embedding, metadata, document, id"

        self._conn.executemany(
            f"""
         INSERT INTO embeddings ({insert_string}) VALUES (?,?,?,?,?,?)""",
            data_to_insert,
        )

        return [uuid.UUID(x[1]) for x in data_to_insert]  # return uuids

    def _count(self, collection_uuid):
        where_string = f"WHERE collection_uuid = '{collection_uuid}'"
        return self._conn.query(f"SELECT COUNT() FROM embeddings {where_string}")

    def count(self, collection_name=None):
        collection_uuid = self.get_collection_uuid_from_name(collection_name)
        return self._count(collection_uuid=collection_uuid).fetchall()[0][0]

    def _format_where(self, where, result):
        for key, value in where.items():
            # Shortcut for $eq
            if type(value) == str:
                result.append(f" json_extract_string(metadata,'$.{key}') = '{value}'")
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
                    raise ValueError(f"Operator {key} not supported with a list of where clauses")

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

    def _get(self, where):
        val = self._conn.execute(
            f"""SELECT {db_schema_to_keys()} FROM embeddings {where}"""
        ).fetchall()
        for i in range(len(val)):
            val[i] = list(val[i])
            val[i][0] = uuid.UUID(val[i][0])
            val[i][1] = uuid.UUID(val[i][1])
            # json.loads metadata
            val[i][5] = json.loads(val[i][5])

        return val

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
            update_fields.append(f"embedding = ?")
        if metadatas is not None:
            update_fields.append(f"metadata = ?")
        if documents is not None:
            update_fields.append(f"document = ?")

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

    def _delete(self, where_str):
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

    def get_by_ids(self, ids=list):
        # select from duckdb table where ids are in the list
        if not isinstance(ids, list):
            raise Exception("ids must be a list")

        if not ids:
            # create an empty pandas dataframe
            return pd.DataFrame()

        return self._conn.execute(
            f"""
            SELECT
                {db_schema_to_keys()}
            FROM
                embeddings
            WHERE
                uuid IN ({','.join([("'" + str(x) + "'") for x in ids])})
        """
        ).fetchall()

    def raw_sql(self, sql):
        return self._conn.execute(sql).df()

    # TODO: This method should share logic with clickhouse impl
    def reset(self):
        self._conn.execute("DROP TABLE collections")
        self._conn.execute("DROP TABLE embeddings")
        self._create_table_collections()
        self._create_table_embeddings()

        self._idx.reset()
        self._idx = Hnswlib(self._settings)

    def __del__(self):
        print("Exiting: Cleaning up .chroma directory")
        self._idx.reset()

    def persist(self):
        raise NotImplementedError(
            "chroma_db_impl='duckdb+parquet' to get persistence functionality"
        )


class PersistentDuckDB(DuckDB):

    _save_folder = None

    def __init__(self, settings):
        super().__init__(settings=settings)

        if settings.persist_directory == ".chroma":
            raise Exception(
                "You cannot use chroma's cache directory, please set a different directory"
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
        print("Persisting DB to disk, putting it in the save folder", self._save_folder)
        if self._conn is None:
            return

        # if the db is empty, dont save
        if self._conn.query(f"SELECT COUNT() FROM embeddings") == 0:
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
        import os

        # load in the embeddings
        if not os.path.exists(f"{self._save_folder}/chroma-embeddings.parquet"):
            print(f"No existing DB found in {self._save_folder}, skipping load")
        else:
            path = self._save_folder + "/chroma-embeddings.parquet"
            self._conn.execute(f"INSERT INTO embeddings SELECT * FROM read_parquet('{path}');")
            print(
                f"""loaded in {self._conn.query(f"SELECT COUNT() FROM embeddings").fetchall()[0][0]} embeddings"""
            )

        # load in the collections
        if not os.path.exists(f"{self._save_folder}/chroma-collections.parquet"):
            print(f"No existing DB found in {self._save_folder}, skipping load")
        else:
            path = self._save_folder + "/chroma-collections.parquet"
            self._conn.execute(f"INSERT INTO collections SELECT * FROM read_parquet('{path}');")
            print(
                f"""loaded in {self._conn.query(f"SELECT COUNT() FROM collections").fetchall()[0][0]} collections"""
            )

    def __del__(self):
        print("PersistentDuckDB del, about to run persist")
        self.persist()

    def reset(self):
        super().reset()
        # empty the save folder
        import shutil
        import os

        shutil.rmtree(self._save_folder)
        os.mkdir(self._save_folder)
