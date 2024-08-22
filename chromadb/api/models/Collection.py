from typing import TYPE_CHECKING, Optional, Union, cast
import numpy as np

from chromadb.api.models.CollectionCommon import CollectionCommon
from chromadb.api.types import (
    URI,
    CollectionMetadata,
    Embedding,
    PyEmbedding,
    Embeddings,
    Include,
    Metadata,
    Document,
    Image,
    Where,
    IDs,
    GetResult,
    AddResult,
    QueryResult,
    ID,
    OneOrMany,
    WhereDocument,
)

import logging

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from chromadb.api import ServerAPI  # noqa: F401


class Collection(CollectionCommon["ServerAPI"]):
    def count(self) -> int:
        """The total number of embeddings added to the database

        Returns:
            int: The total number of embeddings added to the database

        """
        return self._client._count(collection_id=self.id)

    def add(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        embeddings: Optional[ # type: ignore[type-arg]
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> AddResult:
        """Add embeddings to the data store.
        Args:
            ids: The ids of the embeddings you wish to add
            embeddings: The embeddings to add. If None, embeddings will be computed based on the documents or images using the embedding_function set for the Collection. Optional.
            metadatas: The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
            documents: The documents to associate with the embeddings. Optional.
            images: The images to associate with the embeddings. Optional.
            uris: The uris of the images to associate with the embeddings. Optional.

        Returns:
            None

        Raises:
            ValueError: If you don't provide either embeddings or documents
            ValueError: If the length of ids, embeddings, metadatas, or documents don't match
            ValueError: If you don't provide an embedding function and don't provide embeddings
            ValueError: If you provide both embeddings and documents
            ValueError: If you provide an id that already exists

        """
        record_set = self._process_add_request(
            ids=ids if ids is not None else [],
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        result = self._client._add(
            collection_id=self.id,
            ids=record_set["ids"],
            embeddings=cast(Embeddings, record_set["embeddings"]),
            metadatas=record_set["metadatas"],
            documents=record_set["documents"],
            uris=record_set["uris"],
        )

        return result

    def get(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        where: Optional[Where] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents"],  # type: ignore[list-item]
    ) -> GetResult:
        """Get embeddings and their associate data from the data store. If no ids or where filter is provided returns
        all embeddings up to limit starting at offset.

        Args:
            ids: The ids of the embeddings to get. Optional.
            where: A Where type dict used to filter results by. E.g. `{"$and": [{"color" : "red"}, {"price": {"$gte": 4.20}}]}`. Optional.
            limit: The number of documents to return. Optional.
            offset: The offset to start returning results from. Useful for paging results with limit. Optional.
            where_document: A WhereDocument type dict used to filter by the documents. E.g. `{$contains: {"text": "hello"}}`. Optional.
            include: A list of what to include in the results. Can contain `"embeddings"`, `"metadatas"`, `"documents"`. Ids are always included. Defaults to `["metadatas", "documents"]`. Optional.

        Returns:
            GetResult: A GetResult object containing the results.

        """
        (
            valid_ids,
            valid_where,
            valid_where_document,
            valid_include,
        ) = self._validate_and_prepare_get_request(ids, where, where_document, include)

        get_results = self._client._get(
            self.id,
            valid_ids,
            valid_where,
            None,
            limit,
            offset,
            where_document=valid_where_document,
            include=valid_include,
        )

        return self._transform_get_response(get_results, include)

    def peek(self, limit: int = 10) -> GetResult:
        """Get the first few results in the database up to limit

        Args:
            limit: The number of results to return.

        Returns:
            GetResult: A GetResult object containing the results.
        """
        return self._transform_peek_response(self._client._peek(self.id, limit))

    def query(
        self,
        query_embeddings: Optional[  # type: ignore[type-arg]
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        query_texts: Optional[OneOrMany[Document]] = None,
        query_images: Optional[OneOrMany[Image]] = None,
        query_uris: Optional[OneOrMany[URI]] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents", "distances"],  # type: ignore[list-item]
    ) -> QueryResult:
        """Get the n_results nearest neighbor embeddings for provided query_embeddings or query_texts.

        Args:
            query_embeddings: The embeddings to get the closes neighbors of. Optional.
            query_texts: The document texts to get the closes neighbors of. Optional.
            query_images: The images to get the closes neighbors of. Optional.
            query_uris: The URIs to be used with data loader. Optional.
            n_results: The number of neighbors to return for each query_embedding or query_texts. Optional.
            where: A Where type dict used to filter results by. E.g. `{"$and": [{"color" : "red"}, {"price": {"$gte": 4.20}}]}`. Optional.
            where_document: A WhereDocument type dict used to filter by the documents. E.g. `{$contains: {"text": "hello"}}`. Optional.
            include: A list of what to include in the results. Can contain `"embeddings"`, `"metadatas"`, `"documents"`, `"distances"`. Ids are always included. Defaults to `["metadatas", "documents", "distances"]`. Optional.

        Returns:
            QueryResult: A QueryResult object containing the results.

        Raises:
            ValueError: If you don't provide either query_embeddings, query_texts, or query_images
            ValueError: If you provide both query_embeddings and query_texts
            ValueError: If you provide both query_embeddings and query_images
            ValueError: If you provide both query_texts and query_images

        """

        (
            valid_query_embeddings,
            valid_n_results,
            valid_where,
            valid_where_document,
        ) = self._validate_and_prepare_query_request(
            query_embeddings,
            query_texts,
            query_images,
            query_uris,
            n_results,
            where,
            where_document,
            include,
        )

        query_results = self._client._query(
            collection_id=self.id,
            query_embeddings=valid_query_embeddings,
            n_results=valid_n_results,
            where=valid_where,
            where_document=valid_where_document,
            include=include,
        )

        return self._transform_query_response(query_results, include)

    def modify(
        self, name: Optional[str] = None, metadata: Optional[CollectionMetadata] = None
    ) -> None:
        """Modify the collection name or metadata

        Args:
            name: The updated name for the collection. Optional.
            metadata: The updated metadata for the collection. Optional.

        Returns:
            None
        """

        self._validate_modify_request(metadata)

        # Note there is a race condition here where the metadata can be updated
        # but another thread sees the cached local metadata.
        # TODO: fixme
        self._client._modify(id=self.id, new_name=name, new_metadata=metadata)

        self._update_model_after_modify_success(name, metadata)

    def update(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[  # type: ignore[type-arg]
            Union[
                OneOrMany[Embedding],
                OneOrMany[np.ndarray],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> None:
        """Update the embeddings, metadatas or documents for provided ids.

        Args:
            ids: The ids of the embeddings to update
            embeddings: The embeddings to update. If None, embeddings will be computed based on the documents or images using the embedding_function set for the Collection. Optional.
            metadatas:  The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
            documents: The documents to associate with the embeddings. Optional.
            images: The images to associate with the embeddings. Optional.
        Returns:
            None
        """
        record_set = self._process_update_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._client._update(
            collection_id=self.id,
            ids=record_set["ids"],
            embeddings=cast(Embeddings, record_set["embeddings"]),
            metadatas=record_set["metadatas"],
            documents=record_set["documents"],
            uris=record_set["uris"],
        )

    def upsert(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[  # type: ignore[type-arg]
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
        """Update the embeddings, metadatas or documents for provided ids, or create them if they don't exist.

        Args:
            ids: The ids of the embeddings to update
            embeddings: The embeddings to add. If None, embeddings will be computed based on the documents using the embedding_function set for the Collection. Optional.
            metadatas:  The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
            documents: The documents to associate with the embeddings. Optional.

        Returns:
            None
        """
        record_set = self._process_upsert_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._client._upsert(
            collection_id=self.id,
            ids=record_set["ids"],
            embeddings=cast(Embeddings, record_set["embeddings"]),
            metadatas=record_set["metadatas"],
            documents=record_set["documents"],
            uris=record_set["uris"],
        )

    def delete(
        self,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
    ) -> None:
        """Delete the embeddings based on ids and/or a where filter

        Args:
            ids: The ids of the embeddings to delete
            where: A Where type dict used to filter the delection by. E.g. `{"$and": [{"color" : "red"}, {"price": {"$gte": 4.20}]}}`. Optional.
            where_document: A WhereDocument type dict used to filter the deletion by the document content. E.g. `{$contains: {"text": "hello"}}`. Optional.

        Returns:
            None

        Raises:
            ValueError: If you don't provide either ids, where, or where_document
        """
        (ids, where, where_document) = self._validate_and_prepare_delete_request(
            ids, where, where_document
        )

        self._client._delete(self.id, ids, where, where_document)
