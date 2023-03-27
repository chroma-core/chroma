from abc import ABC, abstractmethod
from typing import Dict, List, Sequence, Optional, Tuple
from enum import Enum
from uuid import UUID
import logging
from urllib.parse import urlparse, unquote
import json

import numpy as np
import numpy.typing as npt
import psycopg2
import psycopg2.extras

from chromadb.db import DB
from chromadb.api.types import Embeddings, Documents, IDs, Metadatas, Where, WhereDocument


logger = logging.getLogger(__name__)

COLLECTION_TABLE_SCHEMA = [
    {"uuid": "UUID DEFAULT uuid_generate_v4()"},
    {"name": "TEXT NULL"},
    {"metadata": "JSON"}
]

EMBEDDING_TABLE_SCHEMA_TEMPLATE = [
    {"collection_uuid": "UUID"},
    {"uuid": "UUID DEFAULT uuid_generate_v4()"},
    {"embedding": f"VECTOR({{}})"},
    {"document": "TEXT NULL"},
    {"id": "TEXT NULL"},
    {"metadata": "JSON"},
]

# The `EXTENSIONS_TO_INSTALL` list contains the names of the PostgreSQL extensions that need to be installed.

EXTENSIONS_TO_INSTALL = [
    "vector",     # This extension provides support for vector data types and operations.
    "uuid-ossp"   # This extension provides support for generating UUIDs (Universally Unique Identifiers).
]


class PGExtensionConfigurationException(Exception):
    pass


class DistanceFunction(Enum):
    L2 = 'vector_l2_ops'
    CONSINE = 'vector_cosine_ops'
    INNER_PRODUCT = 'vector_ip_ops'
    
    
class Postgres(DB):
    def __init__(self, settings):
        if not settings.pg_uri:
            raise TypeError("pg_uri is not set")
        self._conn =  self._connect(settings.pg_uri)
        self._settings = settings
        self._initialize_db()
    
    def _connect(self, pg_uri):
        try:
            parsed_uri = urlparse(pg_uri)
            username = unquote(parsed_uri.username)
            password = unquote(parsed_uri.password)
            host = parsed_uri.hostname
            port = parsed_uri.port or 5432
            database = parsed_uri.path.lstrip('/')
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
            )
            return conn
        except psycopg2.Error as e:
            logging.error(f"Exception when connecting to Postgres database using {pg_uri}")
            raise e
        
    def _run_query(self, query, *params):
        if not query:
            return
        
        cursor = self._conn.cursor()
        try:
            logging.debug(f"running query {query} ; with params {params}")
            cursor.execute(query, params)
            self._conn.commit()
        except psycopg2.Error as e:
            logging.error(f"Exception when running query {query}")
            raise e
        return cursor
    
    def _enable_necessary_extensions(self):
        for ext_name in EXTENSIONS_TO_INSTALL:
            cursor = self._run_query(f"CREATE EXTENSION IF NOT EXISTS \"{ext_name}\"")
            cursor.close()
            
    def _create_collections_table(self):
        columns = ", ".join([f"{k} {v}" for column in COLLECTION_TABLE_SCHEMA for k, v in column.items()])
        query = f"CREATE TABLE IF NOT EXISTS collections ({columns})"
        cursor = self._run_query(query)
        cursor.close()
    
    def _create_and_index_embeddings_table(self, dimensionality, space="L2"):
        #  Create embeddings table
        schema = EMBEDDING_TABLE_SCHEMA_TEMPLATE.copy()
        schema[2]["embedding"] = f"VECTOR({dimensionality})"
        columns = ", ".join([f"{k} {v}" for column in schema for k, v in column.items()])
        query = f"CREATE TABLE IF NOT EXISTS embeddings ({columns})"
        cursor = self._run_query(query)
        cursor.close()
        
        # Add index on embeddings column
        embedding_type = DistanceFunction[space].value
        query = f"CREATE INDEX IF NOT EXISTS embeddings_idx ON embeddings USING ivfflat (embedding {embedding_type});"
        cursor = self._run_query(query)
        cursor.close()
        
    
    def _initialize_db(self):
        self._enable_necessary_extensions()
        self._create_collections_table()

    def create_collection(
        self, name: str, metadata: Optional[Dict] = None, get_or_create: bool = False
    ) -> Sequence:
        query = """
            INSERT INTO collections (name, metadata)
            SELECT %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM collections WHERE name = %s)
            RETURNING uuid, name, metadata
        """
        params = (name, json.dumps(metadata), name)
        cursor = self._run_query(query, *params)
        row = cursor.fetchone()
        cursor.close()
        if row:
            return [[row[0], row[1], row[2]]]
        else:
            return []



    def add_incremental(self, collection_uuid: str, ids: List[UUID], embeddings: Embeddings):
        logger.info("Not necessary to add incremental embeddings for Postgres database")
        
    
    def get_collection(self, name: str) -> Sequence:
        query = "SELECT * FROM collections WHERE name = %s LIMIT 1"
        params = (name,)
        cursor = self._run_query(query, *params)
        row = cursor.fetchone()
        cursor.close()
        
        if row:
            return [(row[0], row[1], row[2])]
        else:
            return []

    def list_collections(self) -> Sequence:
        query = "SELECT * FROM collections"
        cursor = self._run_query(query)
        rows = cursor.fetchall()
        cursor.close()
        return [(row[0], row[1], row[2]) for row in rows]

    def update_collection(
        self, current_name: str, new_name: Optional[str] = None, new_metadata: Optional[Dict] = None
    ):
        if new_name is None:
            new_name = current_name
        if new_metadata is None:
            new_metadata = self.get_collection(current_name)[0][2]
        
        query = """UPDATE collections SET name = %s, metadata = %s WHERE name = %s"""
        params = (new_name, json.dumps(new_metadata), name)
        cursor = self._run_query(query, *params)
        cursor.close()

    def delete_collection(self, name: str):
        collection_uuid = self.get_collection_uuid_from_name(name)
        cursor = self._run_query("""SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name=%s)""", ('embeddings',))
        table_exists = cursor.fetchone()[0]
        cursor.close()
        
        if table_exists:
            query = "DELETE FROM embeddings WHERE collection_uuid = %s"
            params = (collection_uuid,)
            cursor = self._run_query(query, *params)
            cursor.close()
            
        
    def get_collection_uuid_from_name(self, collection_name: str) -> str:
        query = "SELECT uuid FROM collections WHERE name=%s"
        params = (collection_name,)
        cursor = self._run_query(query, params)
        row = cursor.fetchone()
        cursor.close()
        if row:
            return row[0]
        else:
            return None

    def add(
        self,
        collection_uuid: str,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
        ids: List[UUID],
    ) -> List[UUID]:
        # Check and setup embeddings table
        cursor = self._run_query("SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name=%s)", ('embeddings',))
        table_exists = cursor.fetchone()[0]
        cursor.close()
        if not table_exists:
            self._create_and_index_embeddings_table(dimensionality=len(embeddings[0]))
            
        data_to_insert = [
            (
                collection_uuid,
                embedding,
                json.dumps(metadatas[i]) if metadatas else None,
                documents[i] if documents else None,
                ids[i],
            )
            for i, embedding in enumerate(embeddings)
        ]

        placeholders = ', '.join(['%s' for _ in range(5)])
        insert_string = "collection_uuid, embedding, metadata, document, id"
        query = f"INSERT INTO embeddings ({insert_string}) VALUES ({placeholders}) RETURNING uuid "
        
        returned_uuids = []

        with self._conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, query, data_to_insert)
            returned_uuids = [row[0] for row in cursor.fetchall()]
        
        self._conn.commit()
        return returned_uuids


    def _get(self, where={}, columns: Optional[List] = None):
        select = f"""SELECT {",".join({columns})}" if columns else "SELECT *"""
        cursor = self._run_query(f"{select} FROM embeddings {where}")
        rows = cursor.fetchall()
        cursor.close()
        return rows
    
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
        if collection_name == None and collection_uuid == None:
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
            where_str += f" ORDER BY collection_uuid" 

        if limit is not None or isinstance(limit, int):
            where_str += f" LIMIT {limit}"

        if offset is not None or isinstance(offset, int):
            where_str += f" OFFSET {offset}"

        return self._get(where=where_str, columns=columns)

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
    
    def update(
        self,
        collection_uuid: str,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
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
            update_fields.append(f"embedding = %s")
        if metadatas is not None:
            update_fields.append(f"metadata = %s")
        if documents is not None:
            update_fields.append(f"document = %s")

        update_statement = f"""
        UPDATE
            embeddings
        SET
            {", ".join(update_fields)}
        WHERE
            id = %s AND
            collection_uuid = '{collection_uuid}';
        """
        cursor = self._run_query(update_statement, *update_data)
        cursor.close()
        

    def count(self, collection_name: str):
        collection_uuid = self.get_collection_uuid_from_name(collection_name)
        return self_count(collection_uuid)

    def _count(self, collection_uuid: str):
        query = f"SELECT COUNT(*) FROM embeddings WHERE collection_uuid = %s"
        params = (collection_uuid,)

        cursor = self._run_query(query, *params)
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            return 0
    
    def delete(
        self,
        where: Where = {},
        collection_name: Optional[str] = None,
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        where_document: WhereDocument = {},
    ):
        if collection_name == None and collection_uuid == None:
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

        return self._delete(where_str)

    def _delete(self):
        query = f"DELETE FROM embeddings {where_str} RETURNING uuid;"
        cursor = self._run_query(query)
        deleted_uuids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return deleted_uuids
        

    def reset(self):
        query = "DROP TABLE collections"
        cursor = self._run_query(query)
        cursor.close()
        
        query = "DROP TABLE embeddings"
        cursor = self._run_query(query)
        cursor.close()
        
        self._initialize_db()

    def _get_index_dims(self, collection_name: str):
        query = "SELECT vector_dims(embedding) FROM embeddings JOIN collections on collections.uuid = embeddings.collection_uuid WHERE collections.name = %s"
        params = (collection_name,)
        return self._run_query(query, params).fetchone()[0]

    
    def _get_doc_length(self, collection_name):
        query = "SELECT COUNT(DISTINCT(id)) FROM embeddings JOIN collections on collections.uuid = embeddings.collection_uuid WHERE collections.name = %s"
        params = (collection_name,)
        return self._run_query(query, params).fetchone()[0]

    def _generate_embedding_distance_select(self, embeddings):
        distances = []
        placeholders = []
        for embedding in embeddings:
            distances.append("SELECT uuid, embedding <-> %s AS distance FROM embeddings WHERE collection_uuid = %s")
            placeholders.append(str(embedding))
        query = ' UNION ALL '.join(distances)
        return query, placeholders
    
    def _get_nearest_neighbors(self, collection_uuid, embeddings, n_results, ids=None):
        select_list = []
        
        select_clauses, placeholders = self._generate_embedding_distance_select(embeddings)
   
        params = [*placeholders, collection_uuid]
        where_clause = ""
        if ids:
            where_clause += f" AND id IN (%s)"
            params.append(tuple(ids))
        
        order_by_limit_clause = f"ORDER BY distance LIMIT %s"
        query = f"{select_clauses} {where_clause} {order_by_limit_clause}"
        params.append(n_results)
        
        cursor = self._run_query(query, *params)
        rows = cursor.fetchall()
        cursor.close()
        uuids = []
        distances = []
        for row in rows:
            uuids.append(row[0])
            distances.append(row[1])
        return [uuids], np.array(distances)
        

    def get_nearest_neighbors(
        self, collection_name, where, embeddings, n_results, where_document
    ) -> Tuple[List[List[UUID]], npt.NDArray]:
        # Either the collection name or the collection uuid must be provided
        if collection_name == None and collection_uuid == None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)
        
        idx_dims = self._get_index_dims(collection_name)
        if idx_dims != len(embeddings[0]):
            raise InvalidDimensionException(
                f"Query embeddings dimensionality {len(embeddings[0])} does not match index dimensionality {idx_dims}"
            )
        
        doc_length = self._get_doc_length(collection_name)
        if n_results > doc_length:
            raise NotEnoughElementsException(
                f"Number of requested results {n_results} cannot be greater than number of elements in index {doc_length}"
            )

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
    
        return self._get_nearest_neighbors(collection_uuid, embeddings, n_results, ids)
    
    def get_by_ids(self, uuids: list, columns=None) -> Sequence:
        if not isinstance(uuids, list):
            raise TypeError(f"Expected ids to be a list, got {uuids}")

        if not uuids:
            # create an empty pandas dataframe
            return pd.DataFrame()
        
        columns = columns + ["uuid"] if columns else ["uuid"]

        select_columns = ' * ' if columns is None else ",".join(columns)
        placeholders = ', '.join(['%s' for _ in uuids])
        query = f"""SELECT {select_columns}
            FROM
                embeddings 
            WHERE 
                uuid IN ({placeholders}) 
            ORDER BY uuid"""
        cursor = self._run_query(query, *uuids)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            new_row = []
            for value in row:
                if isinstance(value, dict):
                    new_row.append(json.dumps(value))
                else:
                    new_row.append(value)
            result.append(tuple(new_row))
        cursor.close()
        return result


    def raw_sql(self, raw_sql):
        cursor = self._run_query(raw_sql)
        cursor.close()

    def create_index(self, collection_uuid: str):
        pass
    
    def has_index(self, collection_name):
        cursor = self._run_query("SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name=%s)", ('embeddings',))
        table_exists = cursor.fetchone()[0]
        cursor.close()
        return table_exists and self.count(collection_name) > 0
    
    def persist(self):
        pass
