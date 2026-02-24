from typing import TYPE_CHECKING, Optional, Union, List, cast, Dict, Any, Tuple

from chromadb.api.models.CollectionCommon import CollectionCommon
from chromadb.api.types import (
    URI,
    CollectionMetadata,
    Embedding,
    PyEmbedding,
    Include,
    IndexingStatus,
    Metadata,
    Document,
    Image,
    Where,
    IDs,
    GetResult,
    QueryResult,
    ID,
    OneOrMany,
    ReadLevel,
    WhereDocument,
    SearchResult,
    maybe_cast_one_to_many,
)
from chromadb.api.collection_configuration import UpdateCollectionConfiguration
from chromadb.execution.expression.plan import Search

import logging

from chromadb.api.functions import Function

if TYPE_CHECKING:
    from chromadb.api.models.AttachedFunction import AttachedFunction

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from chromadb.api import ServerAPI  # noqa: F401


class Collection(CollectionCommon["ServerAPI"]):
    def count(self) -> int:
        """Return the number of records in the collection."""
        return self._client._count(
            collection_id=self.id,
            tenant=self.tenant,
            database=self.database,
        )

    def get_indexing_status(self) -> IndexingStatus:
        """Get the indexing status of this collection.

        Returns:
            IndexingStatus: An object containing:
                - num_indexed_ops: Number of user operations that have been indexed
                - num_unindexed_ops: Number of user operations pending indexing
                - total_ops: Total number of user operations in collection
                - op_indexing_progress: Proportion of user operations that have been indexed as a float between 0 and 1
        """
        return self._client._get_indexing_status(
            collection_id=self.id,
            tenant=self.tenant,
            database=self.database,
        )

    def add(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> None:
        """Add records to the collection.

        Args:
            ids: Record IDs to add.
            embeddings: Embeddings to add. If None, embeddings are computed.
            metadatas: Optional metadata for each record.
            documents: Optional documents for each record.
            images: Optional images for each record.
            uris: Optional URIs for loading images.

        Raises:
            ValueError: If embeddings and documents are both missing.
            ValueError: If embeddings and documents are both provided.
            ValueError: If lengths of provided fields do not match.
            ValueError: If an ID already exists.
        """

        add_request = self._validate_and_prepare_add_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._client._add(
            collection_id=self.id,
            ids=add_request["ids"],
            embeddings=add_request["embeddings"],
            metadatas=add_request["metadatas"],
            documents=add_request["documents"],
            uris=add_request["uris"],
            tenant=self.tenant,
            database=self.database,
        )

    def get(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        where: Optional[Where] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents"],
    ) -> GetResult:
        """Retrieve records from the collection.

        If no filters are provided, returns records up to ``limit`` starting at
        ``offset``.

        Args:
            ids: If provided, only return records with these IDs.
            where: A Where filter used to filter based on metadata values.
            limit: Maximum number of results to return.
            offset: Number of results to skip before returning.
            where_document: A WhereDocument filter used to filter based on K.DOCUMENT.
            include: Fields to include in results. Can contain "embeddings", "metadatas", "documents", "uris". Defaults to "metadatas" and "documents".

        Returns:
            GetResult: Retrieved records and requested fields as a GetResult object.
        """
        get_request = self._validate_and_prepare_get_request(
            ids=ids,
            where=where,
            where_document=where_document,
            include=include,
        )

        get_results = self._client._get(
            collection_id=self.id,
            ids=get_request["ids"],
            where=get_request["where"],
            where_document=get_request["where_document"],
            include=get_request["include"],
            limit=limit,
            offset=offset,
            tenant=self.tenant,
            database=self.database,
        )
        return self._transform_get_response(
            response=get_results, include=get_request["include"]
        )

    def peek(self, limit: int = 10) -> GetResult:
        """Return the first ``limit`` records from the collection.

        Args:
            limit: Maximum number of records to return.

        Returns:
            GetResult: Retrieved records and requested fields.
        """
        return self._transform_peek_response(
            self._client._peek(
                collection_id=self.id,
                n=limit,
                tenant=self.tenant,
                database=self.database,
            )
        )

    def query(
        self,
        query_embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        query_texts: Optional[OneOrMany[Document]] = None,
        query_images: Optional[OneOrMany[Image]] = None,
        query_uris: Optional[OneOrMany[URI]] = None,
        ids: Optional[OneOrMany[ID]] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = [
            "metadatas",
            "documents",
            "distances",
        ],
    ) -> QueryResult:
        """Query for the K nearest neighbor records in the collection.

        This is a batch query API. Multiple queries can be performed at once
        by providing multiple embeddings, texts, or images.

        >>> query_1 = [0.1, 0.2, 0.3]
        >>> query_2 = [0.4, 0.5, 0.6]
        >>> results = collection.query(
        >>>     query_embeddings=[query_1, query_2],
        >>>     n_results=10,
        >>> )

        If query_texts, query_images, or query_uris are provided, the collection's
        embedding function will be used to create embeddings before querying
        the API.

        The `ids`, `where`, `where_document`, and `include` parameters are applied
        to all queries.

        Args:
            query_embeddings: Raw embeddings to query for.
            query_texts: Documents to embed and query against.
            query_images: Images to embed and query against.
            query_uris: URIs to be loaded and embedded.
            ids: Optional subset of IDs to search within.
            n_results: Number of neighbors to return per query.
            where: Metadata filter.
            where_document: Document content filter.
            include: Fields to include in results. Can contain "embeddings", "metadatas", "documents", "uris", "distances". Defaults to "metadatas", "documents", "distances".

        Returns:
            QueryResult: Nearest neighbor results.

        Raises:
            ValueError: If no query input is provided.
            ValueError: If multiple query input types are provided.
        """

        query_request = self._validate_and_prepare_query_request(
            query_embeddings=query_embeddings,
            query_texts=query_texts,
            query_images=query_images,
            query_uris=query_uris,
            ids=ids,
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=include,
        )

        query_results = self._client._query(
            collection_id=self.id,
            ids=query_request["ids"],
            query_embeddings=query_request["embeddings"],
            n_results=query_request["n_results"],
            where=query_request["where"],
            where_document=query_request["where_document"],
            include=query_request["include"],
            tenant=self.tenant,
            database=self.database,
        )

        return self._transform_query_response(
            response=query_results, include=query_request["include"]
        )

    def modify(
        self,
        name: Optional[str] = None,
        metadata: Optional[CollectionMetadata] = None,
        configuration: Optional[UpdateCollectionConfiguration] = None,
    ) -> None:
        """Update collection name, metadata, or configuration.

        Args:
            name: New collection name.
            metadata: New metadata for the collection.
            configuration: New configuration for the collection.
        """

        self._validate_modify_request(metadata)

        # Note there is a race condition here where the metadata can be updated
        # but another thread sees the cached local metadata.
        # TODO: fixme
        self._client._modify(
            id=self.id,
            new_name=name,
            new_metadata=metadata,
            new_configuration=configuration,
            tenant=self.tenant,
            database=self.database,
        )

        self._update_model_after_modify_success(name, metadata, configuration)

    def fork(
        self,
        new_name: str,
    ) -> "Collection":
        """Fork the current collection under a new name. The returning collection should contain identical data to the current collection.
        This is an experimental API that only works for Hosted Chroma for now.

        Args:
            new_name: The name of the new collection.

        Returns:
            Collection: A new collection with the specified name and containing identical data to the current collection.
        """
        model = self._client._fork(
            collection_id=self.id,
            new_name=new_name,
            tenant=self.tenant,
            database=self.database,
        )
        return Collection(
            client=self._client,
            model=model,
            embedding_function=self._embedding_function,
            data_loader=self._data_loader,
        )

    def search(
        self,
        searches: OneOrMany[Search],
        read_level: ReadLevel = ReadLevel.INDEX_AND_WAL,
    ) -> SearchResult:
        """Perform hybrid search on the collection.
        This is an experimental API that only works for distributed and hosted Chroma for now.

        Args:
            searches: A single Search object or a list of Search objects, each containing:
                - where: Where expression for filtering
                - rank: Ranking expression for hybrid search (defaults to Val(0.0))
                - limit: Limit configuration for pagination (defaults to no limit)
                - select: Select configuration for keys to return (defaults to empty)
            read_level: Controls whether to read from the write-ahead log (WAL):
                - ReadLevel.INDEX_AND_WAL: Read from both the compacted index and WAL (default).
                  All committed writes will be visible.
                - ReadLevel.INDEX_ONLY: Read only from the compacted index, skipping the WAL.
                  Faster, but recent writes that haven't been compacted may not be visible.

        Returns:
            SearchResult: Column-major format response with:
                - ids: List of result IDs for each search payload
                - documents: Optional documents for each payload
                - embeddings: Optional embeddings for each payload
                - metadatas: Optional metadata for each payload
                - scores: Optional scores for each payload
                - select: List of selected keys for each payload

        Raises:
            NotImplementedError: For local/segment API implementations

        Examples:
            # Using builder pattern with Key constants
            from chromadb.execution.expression import (
                Search, Key, K, Knn, Val
            )

            # Note: K is an alias for Key, so K.DOCUMENT == Key.DOCUMENT
            search = (Search()
                .where((K("category") == "science") & (K("score") > 0.5))
                .rank(Knn(query=[0.1, 0.2, 0.3]) * 0.8 + Val(0.5) * 0.2)
                .limit(10, offset=0)
                .select(K.DOCUMENT, K.SCORE, "title"))

            # Direct construction
            from chromadb.execution.expression import (
                Search, Eq, And, Gt, Knn, Limit, Select, Key
            )

            search = Search(
                where=And([Eq("category", "science"), Gt("score", 0.5)]),
                rank=Knn(query=[0.1, 0.2, 0.3]),
                limit=Limit(offset=0, limit=10),
                select=Select(keys={Key.DOCUMENT, Key.SCORE, "title"})
            )

            # Single search
            result = collection.search(search)

            # Multiple searches at once
            searches = [
                Search().where(K("type") == "article").rank(Knn(query=[0.1, 0.2])),
                Search().where(K("type") == "paper").rank(Knn(query=[0.3, 0.4]))
            ]
            results = collection.search(searches)

            # Skip WAL for faster queries (may miss recent uncommitted writes)
            from chromadb.api.types import ReadLevel
            result = collection.search(search, read_level=ReadLevel.INDEX_ONLY)
        """
        # Convert single search to list for consistent handling
        searches_list = maybe_cast_one_to_many(searches)
        if searches_list is None:
            searches_list = []

        # Embed any string queries in Knn objects
        embedded_searches = [
            self._embed_search_string_queries(search) for search in searches_list
        ]

        return self._client._search(
            collection_id=self.id,
            searches=cast(List[Search], embedded_searches),
            tenant=self.tenant,
            database=self.database,
            read_level=read_level,
        )

    def update(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> None:
        """Update existing records by ID.

        Records are provided in columnar format. If provided, the `embeddings`, `metadatas`, `documents`, and `uris` lists must be the same length.
        Entries in each list correspond to the same record.

        >>> ids = ["id1", "id2", "id3"]
        >>> embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]]
        >>> metadatas = [{"key": "value"}, {"key": "value"}, {"key": "value"}]
        >>> documents = ["document1", "document2", "document3"]
        >>> uris = ["uri1", "uri2", "uri3"]
        >>> collection.update(ids, embeddings, metadatas, documents, uris)

        If `embeddings` are not provided, the embeddings will be computed based on `documents` using the collection's embedding function.

        Args:
            ids: Record IDs to update.
            embeddings: Updated embeddings. If None, embeddings are computed.
            metadatas: Updated metadata.
            documents: Updated documents.
            images: Updated images.
            uris: Updated URIs for loading images.
        """
        update_request = self._validate_and_prepare_update_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._client._update(
            collection_id=self.id,
            ids=update_request["ids"],
            embeddings=update_request["embeddings"],
            metadatas=update_request["metadatas"],
            documents=update_request["documents"],
            uris=update_request["uris"],
            tenant=self.tenant,
            database=self.database,
        )

    def upsert(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> None:
        """Create or update records by ID.

        Args:
            ids: Record IDs to upsert.
            embeddings: Embeddings to add or update. If None, embeddings are computed.
            metadatas: Metadata to add or update.
            documents: Documents to add or update.
            images: Images to add or update.
            uris: URIs for loading images.
        """
        upsert_request = self._validate_and_prepare_upsert_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._client._upsert(
            collection_id=self.id,
            ids=upsert_request["ids"],
            embeddings=upsert_request["embeddings"],
            metadatas=upsert_request["metadatas"],
            documents=upsert_request["documents"],
            uris=upsert_request["uris"],
            tenant=self.tenant,
            database=self.database,
        )

    def delete(
        self,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
    ) -> None:
        """Delete records by ID or filters.

        All documents that match the `ids` or `where` and `where_document` filters will be deleted.

        Args:
            ids: Record IDs to delete.
            where: Metadata filter.
            where_document: Document content filter.

        Raises:
            ValueError: If no IDs or filters are provided.
        """
        delete_request = self._validate_and_prepare_delete_request(
            ids, where, where_document
        )

        self._client._delete(
            collection_id=self.id,
            ids=delete_request["ids"],
            where=delete_request["where"],
            where_document=delete_request["where_document"],
            tenant=self.tenant,
            database=self.database,
        )

    def attach_function(
        self,
        function: Function,
        name: str,
        output_collection: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple["AttachedFunction", bool]:
        """Attach a function to this collection.

        Args:
            function: A Function enum value (e.g., STATISTICS_FUNCTION, RECORD_COUNTER_FUNCTION)
            name: Unique name for this attached function
            output_collection: Name of the collection where function output will be stored
            params: Optional dictionary with function-specific parameters

        Returns:
            Tuple of (AttachedFunction, created) where created is True if newly created,
            False if already existed (idempotent request)

        Example:
            >>> from chromadb.api.functions import STATISTICS_FUNCTION
            >>> attached_fn = collection.attach_function(
            ...     function=STATISTICS_FUNCTION,
            ...     name="mycoll_stats_fn",
            ...     output_collection="mycoll_stats",
            ... )
            >>> if created:
            ...     print("New function attached")
            ... else:
            ...     print("Function already existed")
        """
        function_id = function.value if isinstance(function, Function) else function
        return self._client.attach_function(
            function_id=function_id,
            name=name,
            input_collection_id=self.id,
            output_collection=output_collection,
            params=params,
            tenant=self.tenant,
            database=self.database,
        )

    def get_attached_function(self, name: str) -> "AttachedFunction":
        """Get an attached function by name for this collection.

        Args:
            name: Name of the attached function

        Returns:
            AttachedFunction: The attached function object

        Raises:
            NotFoundError: If the attached function doesn't exist
        """
        return self._client.get_attached_function(
            name=name,
            input_collection_id=self.id,
            tenant=self.tenant,
            database=self.database,
        )

    def detach_function(
        self,
        name: str,
        delete_output_collection: bool = False,
    ) -> bool:
        """Detach a function from this collection.

        Args:
            name: The name of the attached function
            delete_output_collection: Whether to also delete the output collection. Defaults to False.

        Returns:
            bool: True if successful

        Example:
            >>> success = collection.detach_function("my_function", delete_output_collection=True)
        """
        return self._client.detach_function(
            name=name,
            input_collection_id=self.id,
            delete_output=delete_output_collection,
            tenant=self.tenant,
            database=self.database,
        )
