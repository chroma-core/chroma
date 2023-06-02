from abc import ABC, abstractmethod
from typing import Sequence, Optional
import pandas as pd
from uuid import UUID
from chromadb.api.models.Collection import Collection
from chromadb.api.types import (
    CollectionMetadata,
    Documents,
    EmbeddingFunction,
    Embeddings,
    IDs,
    Include,
    Metadatas,
    Where,
    QueryResult,
    GetResult,
    WhereDocument,
)
from chromadb.config import Component
import chromadb.utils.embedding_functions as ef
from overrides import override


class API(Component, ABC):
    @abstractmethod
    def heartbeat(self) -> int:
        """Returns the current server time in nanoseconds to check if the server is alive

        Args:
            None

        Returns:
            int: The current server time in nanoseconds

        """
        pass

    @abstractmethod
    def list_collections(self) -> Sequence[Collection]:
        """Returns all collections in the database

        Args:
            None

        Returns:
            dict: A dictionary of collections

        """
        pass

    @abstractmethod
    def create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
        get_or_create: bool = False,
    ) -> Collection:
        """Creates a new collection in the database

        Args:
            name  The name of the collection to create. The name must be unique.
            metadata: A dictionary of metadata to associate with the collection. Defaults to None.
            embedding_function: A function that takes documents and returns an embedding. Defaults to None.
            get_or_create: If True, will return the collection if it already exists,
                and update the metadata (if applicable). Defaults to False.

        Returns:
            dict: the created collection

        """
        pass

    @abstractmethod
    def delete_collection(
        self,
        name: str,
    ) -> None:
        """Deletes a collection from the database

        Args:
            name: The name of the collection to delete
        """

    @abstractmethod
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
    ) -> Collection:
        """Calls create_collection with get_or_create=True.
           If the collection exists, but with different metadata, the metadata will be replaced.

        Args:
            name: The name of the collection to create. The name must be unique.
            metadata: A dictionary of metadata to associate with the collection. Defaults to None.
            embedding_function: A function that takes documents and returns an embedding. Should be the same as the one used to create the collection. Defaults to None.
        Returns:
            the created collection

        """
        pass

    @abstractmethod
    def get_collection(
        self,
        name: str,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
    ) -> Collection:
        """Gets a collection from the database by either name or uuid

        Args:
            name: The name of the collection to get. Defaults to None.
            embedding_function: A function that takes documents and returns an embedding. Should be the same as the one used to create the collection. Defaults to None.

        Returns:
            dict: the requested collection

        """
        pass

    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        """Modify a collection in the database - can update the name and/or metadata

        Args:
            current_name: The name of the collection to modify
            new_name: The new name of the collection. Defaults to None.
            new_metadata: The new metadata to associate with the collection. Defaults to None.
        """
        pass

    @abstractmethod
    def _add(
        self,
        ids: IDs,
        collection_id: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ) -> bool:
        """Add embeddings to the data store. This is the most general way to add embeddings to the database.
        ⚠️ It is recommended to use the more specific methods below when possible.

        Args:
            collection_id: The collection to add the embeddings to
            embedding: The sequence of embeddings to add
            metadata: The metadata to associate with the embeddings. Defaults to None.
            documents: The documents to associate with the embeddings. Defaults to None.
            ids: The ids to associate with the embeddings. Defaults to None.
        """
        pass

    @abstractmethod
    def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ) -> bool:
        """Add embeddings to the data store. This is the most general way to add embeddings to the database.
        ⚠️ It is recommended to use the more specific methods below when possible.

        Args:
            collection_id: The collection to add the embeddings to
            embedding: The sequence of embeddings to add
        """
        pass

    @abstractmethod
    def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ) -> bool:
        """Add or update entries in the embedding store.
        If an entry with the same id already exists, it will be updated, otherwise it will be added.

        Args:
            collection_id: The collection to add the embeddings to
            ids: The ids to associate with the embeddings. Defaults to None.
            embeddings: The sequence of embeddings to add
            metadatas: The metadata to associate with the embeddings. Defaults to None.
            documents: The documents to associate with the embeddings. Defaults to None.
            increment_index: If True, will incrementally add to the ANN index of the collection. Defaults to True.
        """
        pass

    @abstractmethod
    def _count(self, collection_id: UUID) -> int:
        """Returns the number of embeddings in the database

        Args:
            collection_id: The collection to count the embeddings in.


        Returns:
            int: The number of embeddings in the collection

        """
        pass

    @abstractmethod
    def _peek(self, collection_id: UUID, n: int = 10) -> GetResult:
        pass

    @abstractmethod
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
        """Gets embeddings from the database. Supports filtering, sorting, and pagination.
        ⚠️ This method should not be used directly.

        Args:
            where: A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.
            sort: The column to sort the embeddings by. Defaults to None.
            limit: The maximum number of embeddings to return. Defaults to None.
            offset: The number of embeddings to skip before returning. Defaults to None.
            page: The page number to return. Defaults to None.
            page_size: The number of embeddings to return per page. Defaults to None.

        Returns:
            pd.DataFrame: A pandas dataframe containing the embeddings and metadata

        """
        pass

    @abstractmethod
    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs],
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
    ) -> IDs:
        """Deletes embeddings from the database
        ⚠️ This method should not be used directly.

        Args:
            where: A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.

        Returns:
            List: The list of internal UUIDs of the deleted embeddings
        """
        pass

    @abstractmethod
    def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Where = {},
        where_document: WhereDocument = {},
        include: Include = ["embeddings", "metadatas", "documents", "distances"],
    ) -> QueryResult:
        """Gets the nearest neighbors of a single embedding
        ⚠️ This method should not be used directly.

        Args:
            embedding: The embedding to find the nearest neighbors of
            n_results: The number of nearest neighbors to return. Defaults to 10.
            where: A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.
        """
        pass

    @override
    @abstractmethod
    def reset(self) -> None:
        """Resets the database
        ⚠️ This is destructive and will delete all data in the database.
        Args:
            None

        Returns:
            None
        """
        pass

    @abstractmethod
    def raw_sql(self, sql: str) -> pd.DataFrame:
        """Runs a raw SQL query against the database
        ⚠️ This method should not be used directly.

        Args:
            sql: The SQL query to run

        Returns:
            pd.DataFrame: A pandas dataframe containing the results of the query
        """
        pass

    @abstractmethod
    def create_index(self, collection_name: str) -> bool:
        """Creates an index for the given collection
        ⚠️ This method should not be used directly.

        Args:
            collection_name: The collection to create the index for. Uses the client's collection if None. Defaults to None.

        Returns:
            bool: True if the index was created successfully

        """
        pass

    @abstractmethod
    def persist(self) -> bool:
        """Persist the database to disk"""
        pass

    @abstractmethod
    def get_version(self) -> str:
        """Get the version of Chroma.

        Returns:
            str: The version of Chroma

        """
        pass
