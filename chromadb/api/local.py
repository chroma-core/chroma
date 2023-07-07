import json
import time
from uuid import UUID
from typing import List, Optional, Sequence, cast
from chromadb import __version__
from chromadb.api import API
from chromadb.db import DB
from chromadb.api.types import (
    Documents,
    EmbeddingFunction,
    Embeddings,
    GetResult,
    IDs,
    Include,
    Metadata,
    Metadatas,
    QueryResult,
    Where,
    WhereDocument,
    CollectionMetadata,
    validate_metadata,
)
from chromadb.api.models.Collection import Collection
from chromadb.config import System
import chromadb.utils.embedding_functions as ef
import re

from chromadb.telemetry import Telemetry
from chromadb.telemetry.events import CollectionAddEvent, CollectionDeleteEvent
from overrides import override
import pandas as pd
import logging

logger = logging.getLogger(__name__)


# mimics s3 bucket requirements for naming
def check_index_name(index_name: str) -> None:
    msg = (
        "Expected collection name that "
        "(1) contains 3-63 characters, "
        "(2) starts and ends with an alphanumeric character, "
        "(3) otherwise contains only alphanumeric characters, underscores or hyphens (-), "
        "(4) contains no two consecutive periods (..) and "
        "(5) is not a valid IPv4 address, "
        f"got {index_name}"
    )
    if len(index_name) < 3 or len(index_name) > 63:
        raise ValueError(msg)
    if not re.match("^[a-zA-Z0-9][a-zA-Z0-9._-]*[a-zA-Z0-9]$", index_name):
        raise ValueError(msg)
    if ".." in index_name:
        raise ValueError(msg)
    if re.match("^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$", index_name):
        raise ValueError(msg)


class LocalAPI(API):
    _db: DB
    _telemetry_client: Telemetry

    def __init__(self, system: System):
        super().__init__(system)
        self._db = self.require(DB)
        self._telemetry_client = self.require(Telemetry)

    @override
    def heartbeat(self) -> int:
        """Ping the database to ensure it is alive

        Returns:
            The current time in nanoseconds since epoch

        """
        return int(time.time_ns())

    #
    # COLLECTION METHODS
    #
    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
        get_or_create: bool = False,
    ) -> Collection:
        """Create a new collection with the given name and metadata.
        Args:
            name: The name of the collection to create
            metadata: Optional metadata to associate with the collection
            embedding_function: Optional function to use to embed documents
            get_or_create: If True, return the existing collection if it exists

        Returns:
            The newly created collection

        Raises:
            ValueError: If the collection already exists and get_or_create is False
            ValueError: If the collection name is invalid

        Examples:
            ```python
            client.create_collection("my_collection")
            # collection(name="my_collection", metadata={})

            client.create_collection("my_collection", metadata={"foo": "bar"})
            # collection(name="my_collection", metadata={"foo": "bar"})
            ```
        """
        check_index_name(name)

        if metadata is not None:
            validate_metadata(metadata)

        res = self._db.create_collection(name, metadata, get_or_create)
        return Collection(
            client=self,
            name=name,
            embedding_function=embedding_function,
            id=res[0][0],
            metadata=res[0][2],
        )

    @override
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
    ) -> Collection:
        """Get or create a collection with the given name and metadata.
        Args:
            name: The name of the collection to get or create
            metadata: Optional metadata to associate with the collection
            embedding_function: Optional function to use to embed documents

        Returns:
            The collection

        Examples:
            ```python
            client.get_or_create_collection("my_collection")
            # collection(name="my_collection", metadata={})
            ```
        """

        if metadata is not None:
            validate_metadata(metadata)

        return self.create_collection(
            name, metadata, embedding_function, get_or_create=True
        )

    @override
    def get_collection(
        self,
        name: str,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
    ) -> Collection:
        """Get a collection with the given name.
        Args:
            name: The name of the collection to get
            embedding_function: Optional function to use to embed documents

        Returns:
            The collection

        Raises:
            ValueError: If the collection does not exist

        Examples:
            ```python
            client.get_collection("my_collection")
            # collection(name="my_collection", metadata={})
            ```
        """
        res = self._db.get_collection(name)
        if len(res) == 0:
            raise ValueError(f"Collection {name} does not exist")
        return Collection(
            client=self,
            name=name,
            id=res[0][0],
            embedding_function=embedding_function,
            metadata=res[0][2],
        )

    @override
    def list_collections(self) -> Sequence[Collection]:
        """List all collections.
        Returns:
            A list of collections

        Examples:
            ```python
            client.list_collections()
            # [collection(name="my_collection", metadata={})]
            ```
        """
        collections = []
        db_collections = self._db.list_collections()
        for db_collection in db_collections:
            collections.append(
                Collection(
                    client=self,
                    id=db_collection[0],
                    name=db_collection[1],
                    metadata=db_collection[2],
                )
            )
        return collections

    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        if new_name is not None:
            check_index_name(new_name)

        self._db.update_collection(id, new_name, new_metadata)

    @override
    def delete_collection(self, name: str) -> None:
        """Delete a collection with the given name.
        Args:
            name: The name of the collection to delete

        Raises:
            ValueError: If the collection does not exist

        Examples:
            ```python
            client.delete_collection("my_collection")
            ```
        """
        self._db.delete_collection(name)

    #
    # ITEM METHODS
    #
    @override
    def _add(
        self,
        ids: IDs,
        collection_id: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ) -> bool:
        existing_ids = set(self._get(collection_id, ids=ids, include=[])["ids"])
        if len(existing_ids) > 0:
            logger.info(f"Adding {len(existing_ids)} items with ids that already exist")
            # Partially add the items that don't already exist
            valid_indices = [i for i, id in enumerate(ids) if id not in existing_ids]
            if len(valid_indices) == 0:
                return False
            filtered_ids: IDs = []
            filtered_embeddings: Embeddings = []
            if metadatas is not None:
                filtered_metadatas: Metadatas = []
            if documents is not None:
                filtered_documents: Documents = []
            for index in valid_indices:
                filtered_ids.append(ids[index])
                filtered_embeddings.append(embeddings[index])
                if metadatas is not None:
                    filtered_metadatas.append(metadatas[index])
                if documents is not None:
                    filtered_documents.append(documents[index])
            ids = filtered_ids
            embeddings = filtered_embeddings
            if metadatas is not None:
                metadatas = filtered_metadatas
            if documents is not None:
                documents = filtered_documents

        added_uuids = self._db.add(
            collection_id,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            ids=ids,
        )

        if increment_index:
            self._db.add_incremental(collection_id, added_uuids, embeddings)

        self._telemetry_client.capture(CollectionAddEvent(str(collection_id), len(ids)))
        return True  # NIT: should this return the ids of the succesfully added items?

    @override
    def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ) -> bool:
        self._db.update(collection_id, ids, embeddings, metadatas, documents)
        return True

    @override
    def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ) -> bool:
        # Determine which ids need to be added and which need to be updated based on the ids already in the collection
        existing_ids = set(self._get(collection_id, ids=ids, include=[])["ids"])

        ids_to_add = []
        ids_to_update = []
        embeddings_to_add: Embeddings = []
        embeddings_to_update: Embeddings = []
        metadatas_to_add: Optional[Metadatas] = [] if metadatas else None
        metadatas_to_update: Optional[Metadatas] = [] if metadatas else None
        documents_to_add: Optional[Documents] = [] if documents else None
        documents_to_update: Optional[Documents] = [] if documents else None

        for i, id in enumerate(ids):
            if id in existing_ids:
                ids_to_update.append(id)
                if embeddings is not None:
                    embeddings_to_update.append(embeddings[i])
                if metadatas is not None:
                    metadatas_to_update.append(metadatas[i])  # type: ignore
                if documents is not None:
                    documents_to_update.append(documents[i])  # type: ignore
            else:
                ids_to_add.append(id)
                if embeddings is not None:
                    embeddings_to_add.append(embeddings[i])
                if metadatas is not None:
                    metadatas_to_add.append(metadatas[i])  # type: ignore
                if documents is not None:
                    documents_to_add.append(documents[i])  # type: ignore

        if len(ids_to_add) > 0:
            self._add(
                ids_to_add,
                collection_id,
                embeddings_to_add,
                metadatas_to_add,
                documents_to_add,
                increment_index=increment_index,
            )

        if len(ids_to_update) > 0:
            self._update(
                collection_id,
                ids_to_update,
                embeddings_to_update,
                metadatas_to_update,
                documents_to_update,
            )
        self._db.update(collection_id, ids, embeddings, metadatas, documents)

        return True

    @override
    def _get(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        where_document: Optional[WhereDocument] = {},
        include: Include = ["embeddings", "metadatas", "documents"],
    ) -> GetResult:
        if where is None:
            where = {}

        if where_document is None:
            where_document = {}

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        include_embeddings = "embeddings" in include
        include_documents = "documents" in include
        include_metadatas = "metadatas" in include

        # Remove plural from include since db columns are singular
        db_columns = [column[:-1] for column in include] + ["id"]
        column_index = {
            column_name: index for index, column_name in enumerate(db_columns)
        }

        db_result = self._db.get(
            collection_uuid=collection_id,
            ids=ids,
            where=where,
            sort=sort,
            limit=limit,
            offset=offset,
            where_document=where_document,
            columns=db_columns,
        )

        get_result = GetResult(
            ids=[],
            embeddings=[] if include_embeddings else None,
            documents=[] if include_documents else None,
            metadatas=[] if include_metadatas else None,
        )

        for entry in db_result:
            if include_embeddings:
                cast(List, get_result["embeddings"]).append(  # type: ignore
                    entry[column_index["embedding"]]
                )
            if include_documents:
                cast(List, get_result["documents"]).append(  # type: ignore
                    entry[column_index["document"]]
                )
            if include_metadatas:
                cast(List, get_result["metadatas"]).append(  # type: ignore
                    entry[column_index["metadata"]]
                )
            get_result["ids"].append(entry[column_index["id"]])
        return get_result

    @override
    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
    ) -> IDs:
        if where is None:
            where = {}

        if where_document is None:
            where_document = {}

        deleted_uuids = self._db.delete(
            collection_uuid=collection_id,
            where=where,
            ids=ids,
            where_document=where_document,
        )
        self._telemetry_client.capture(
            CollectionDeleteEvent(str(collection_id), len(deleted_uuids))
        )

        return deleted_uuids

    @override
    def _count(self, collection_id: UUID) -> int:
        return self._db.count(collection_id)

    @override
    def reset(self) -> bool:
        """Reset the database. This will delete all collections and items.

        Returns:
            True if the database was reset successfully

        """
        self._db.reset_state()
        return True

    @override
    def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Where = {},
        where_document: WhereDocument = {},
        include: Include = ["documents", "metadatas", "distances"],
    ) -> QueryResult:
        uuids, distances = self._db.get_nearest_neighbors(
            collection_uuid=collection_id,
            where=where,
            where_document=where_document,
            embeddings=query_embeddings,
            n_results=n_results,
        )

        include_embeddings = "embeddings" in include
        include_documents = "documents" in include
        include_metadatas = "metadatas" in include
        include_distances = "distances" in include

        query_result = QueryResult(
            ids=[],
            embeddings=[] if include_embeddings else None,
            documents=[] if include_documents else None,
            metadatas=[] if include_metadatas else None,
            distances=[] if include_distances else None,
        )
        for i in range(len(uuids)):
            embeddings: Embeddings = []
            documents: Documents = []
            ids: IDs = []
            metadatas: List[Optional[Metadata]] = []
            # Remove plural from include since db columns are singular
            db_columns = [
                column[:-1] for column in include if column != "distances"
            ] + ["id"]
            column_index = {
                column_name: index for index, column_name in enumerate(db_columns)
            }
            db_result = self._db.get_by_ids(uuids[i], columns=db_columns)

            for entry in db_result:
                if include_embeddings:
                    embeddings.append(entry[column_index["embedding"]])
                if include_documents:
                    documents.append(entry[column_index["document"]])
                if include_metadatas:
                    metadatas.append(
                        json.loads(entry[column_index["metadata"]])
                        if entry[column_index["metadata"]]
                        else None
                    )
                ids.append(entry[column_index["id"]])

            if include_embeddings:
                cast(List[Embeddings], query_result["embeddings"]).append(embeddings)
            if include_documents:
                cast(List[Documents], query_result["documents"]).append(documents)
            if include_metadatas:
                cast(List[List[Optional[Metadata]]], query_result["metadatas"]).append(
                    metadatas
                )
            if include_distances:
                cast(List[float], query_result["distances"]).append(distances[i])
            query_result["ids"].append(ids)

        return query_result

    @override
    def raw_sql(self, sql: str) -> pd.DataFrame:
        return self._db.raw_sql(sql)  # type: ignore

    @override
    def create_index(self, collection_name: str) -> bool:
        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)
        self._db.create_index(collection_uuid=collection_uuid)
        return True

    @override
    def _peek(self, collection_id: UUID, n: int = 10) -> GetResult:
        return self._get(
            collection_id=collection_id,
            limit=n,
            include=["embeddings", "documents", "metadatas"],
        )

    @override
    def persist(self) -> bool:
        """Persist the database to disk.

        Returns:
            True if the database was persisted successfully

        """
        self._db.persist()
        return True

    @override
    def get_version(self) -> str:
        """Get the version of Chroma.

        Returns:
            The version of Chroma

        """
        return __version__
