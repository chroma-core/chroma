from abc import ABC, abstractmethod
from typing import Union, Sequence, Optional, TypedDict, List, Dict
from uuid import UUID
import pandas as pd
from chromadb.api.models.Collection import Collection
from chromadb.api.types import (
    ID,
    Documents,
    Embeddings,
    IDs,
    Metadatas,
    Where,
    QueryResult,
    GetResult,
    WhereDocument,
)
import json


class API(ABC):
    @abstractmethod
    def __init__(self):
        pass

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
        metadata: Optional[Dict] = None,
    ) -> Collection:
        """Creates a new collection in the database

        Args:
            name (str): The name of the collection to create. The name must be unique.
            metadata (Optional[Dict], optional): A dictionary of metadata to associate with the collection. Defaults to None.

        Returns:
            dict: the created collection

        """
        pass

    @abstractmethod
    def get_collection(
        self,
        name: Optional[str] = None,
        uuid: Optional[UUID] = None,
    ) -> Collection:
        """Gets a collection from the database by either name or uuid

        Args:
            name (Optional[str]): The name of the collection to get. Defaults to None.
            the uuid (Optional[UUID]): The uuid of the collection to get. Defaults to None.

        Returns:
            dict: the requested collection

        """
        pass

    def _modify(
        self,
        current_name: str,
        new_name: Optional[str] = None,
        new_metadata: Optional[Dict] = None,
    ):
        pass

    @abstractmethod
    def _add(
        self,
        ids: IDs,
        collection_name: Union[str, Sequence[str]],
        embedding: Optional[Embeddings],
        metadata: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ):
        """Add embeddings to the data store. This is the most general way to add embeddings to the database.
        ⚠️ It is recommended to use the more specific methods below when possible.

        Args:
            collection_name (Union[str, Sequence[str]]): The model space(s) to add the embeddings to
            embedding (Sequence[Sequence[float]]): The sequence of embeddings to add
            metadata (Optional[Union[Dict, Sequence[Dict]]], optional): The metadata to associate with the embeddings. Defaults to None.
            documents (Optional[Union[str, Sequence[str]]], optional): The documents to associate with the embeddings. Defaults to None.
            ids (Optional[Union[str, Sequence[str]]], optional): The ids to associate with the embeddings. Defaults to None.
        """
        pass

    @abstractmethod
    def _update(
        self,
        collection_name: str,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ):
        """Add embeddings to the data store. This is the most general way to add embeddings to the database.
        ⚠️ It is recommended to use the more specific methods below when possible.

        Args:
            collection_name (Union[str, Sequence[str]]): The model space(s) to add the embeddings to
            embedding (Sequence[Sequence[float]]): The sequence of embeddings to add
        """
        pass

    @abstractmethod
    def _count(self, collection_name: Optional[str] = None) -> int:
        """Returns the number of embeddings in the database

        Args:
            collection_name (Optional[str], optional): The model space to count the embeddings in. If None (default), returns the total count of all embeddings.

        Returns:
            int: The number of embeddings in the database

        """
        pass

    @abstractmethod
    def _peek(self, collection_name: str, n: int = 10) -> GetResult:
        pass

    @abstractmethod
    def _get(
        self,
        collection_name: str,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        where_document: Optional[WhereDocument] = {},
    ) -> GetResult:

        """Gets embeddings from the database. Supports filtering, sorting, and pagination.
        ⚠️ This method should not be used directly.

        Args:
            where (Optional[Dict[str, str]], optional): A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.
            sort (Optional[str], optional): The column to sort the embeddings by. Defaults to None.
            limit (Optional[int], optional): The maximum number of embeddings to return. Defaults to None.
            offset (Optional[int], optional): The number of embeddings to skip before returning. Defaults to None.
            page (Optional[int], optional): The page number to return. Defaults to None.
            page_size (Optional[int], optional): The number of embeddings to return per page. Defaults to None.

        Returns:
            pd.DataFrame: A pandas dataframe containing the embeddings and metadata

        """
        pass

    @abstractmethod
    def _delete(self, collection_name: str, ids: Optional[IDs], where: Optional[Where] = {}, where_document: Optional[WhereDocument] = {}):
        """Deletes embeddings from the database
        ⚠️ This method should not be used directly.

        Args:
            where (Optional[Dict[str, str]], optional): A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.
        """
        pass

    @abstractmethod
    def _query(
        self,
        collection_name: str,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Where = {},
        where_document: WhereDocument = {},
    ) -> QueryResult:
        """Gets the nearest neighbors of a single embedding
        ⚠️ This method should not be used directly.

        Args:
            embedding (Sequence[float]): The embedding to find the nearest neighbors of
            n_results (int, optional): The number of nearest neighbors to return. Defaults to 10.
            where (Dict[str, str], optional): A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.
        """
        pass

    @abstractmethod
    def reset(self) -> bool:
        """Resets the database
        ⚠️ This is destructive and will delete all data in the database.
        Args:
            None

        Returns:
            bool: True if the reset was successful
        """
        pass

    @abstractmethod
    def raw_sql(self, sql: str) -> pd.DataFrame:
        """Runs a raw SQL query against the database
        ⚠️ This method should not be used directly.

        Args:
            sql (str): The SQL query to run

        Returns:
            pd.DataFrame: A pandas dataframe containing the results of the query
        """
        pass

    @abstractmethod
    def create_index(self, collection_name: Optional[str] = None) -> bool:
        """Creates an index for the given model space
        ⚠️ This method should not be used directly.

        Args:
            collection_name (Optional[str], optional): The model space to create the index for. Uses the client's model space if None. Defaults to None.

        Returns:
            bool: True if the index was created successfully

        """
        pass
