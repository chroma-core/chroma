from typing import TYPE_CHECKING, Optional, Union
import numpy as np

from chromadb.api.models.CollectionCommon import CollectionCommon
from chromadb.api.types import (
    URI,
    CollectionMetadata,
    Embedding,
    Include,
    Metadata,
    Document,
    Image,
    Where,
    IDs,
    GetResult,
    QueryResult,
    ID,
    OneOrMany,
    WhereDocument,
    maybe_cast_one_to_many_ids,
    maybe_cast_one_to_many_embedding,
    maybe_cast_one_to_many_document,
    maybe_cast_one_to_many_image,
    maybe_cast_one_to_many_uri,
    validate_ids,
    validate_include,
    validate_metadata,
    validate_where,
    validate_where_document,
    validate_n_results,
    validate_embeddings,
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
        ids: OneOrMany[ID],
        embeddings: Optional[
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
        (
            ids,
            embeddings,
            metadatas,
            documents,
            images,
            uris,
        ) = self._validate_embedding_set(
            ids, embeddings, metadatas, documents, images, uris
        )

        # We need to compute the embeddings if they're not provided
        if embeddings is None:
            # At this point, we know that one of documents or images are provided from the validation above
            if documents is not None:
                embeddings = self._embed(input=documents)
            elif images is not None:
                embeddings = self._embed(input=images)
            else:
                if uris is None:
                    raise ValueError(
                        "You must provide either embeddings, documents, images, or uris."
                    )
                if self._data_loader is None:
                    raise ValueError(
                        "You must set a data loader on the collection if loading from URIs."
                    )
                embeddings = self._embed(self._data_loader(uris))

        self._client._add(ids, self.id, embeddings, metadatas, documents, uris)

    def get(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        where: Optional[Where] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents"],
    ) -> GetResult:
        """Get embeddings and their associate data from the data store. If no ids or where filter is provided returns
        all embeddings up to limit starting at offset.

        Args:
            ids: The ids of the embeddings to get. Optional.
            where: A Where type dict used to filter results by. E.g. `{"$and": ["color" : "red", "price": {"$gte": 4.20}]}`. Optional.
            limit: The number of documents to return. Optional.
            offset: The offset to start returning results from. Useful for paging results with limit. Optional.
            where_document: A WhereDocument type dict used to filter by the documents. E.g. `{$contains: {"text": "hello"}}`. Optional.
            include: A list of what to include in the results. Can contain `"embeddings"`, `"metadatas"`, `"documents"`. Ids are always included. Defaults to `["metadatas", "documents"]`. Optional.

        Returns:
            GetResult: A GetResult object containing the results.

        """
        valid_where = validate_where(where) if where else None
        valid_where_document = (
            validate_where_document(where_document) if where_document else None
        )
        valid_ids = validate_ids(maybe_cast_one_to_many_ids(ids)) if ids else None
        valid_include = validate_include(include, allow_distances=False)

        if "data" in include and self._data_loader is None:
            raise ValueError(
                "You must set a data loader on the collection if loading from URIs."
            )

        # We need to include uris in the result from the API to load datas
        if "data" in include and "uris" not in include:
            valid_include.append("uris")

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

        if (
            "data" in include
            and self._data_loader is not None
            and get_results["uris"] is not None
        ):
            get_results["data"] = self._data_loader(get_results["uris"])

        # Remove URIs from the result if they weren't requested
        if "uris" not in include:
            get_results["uris"] = None

        return get_results

    def peek(self, limit: int = 10) -> GetResult:
        """Get the first few results in the database up to limit

        Args:
            limit: The number of results to return.

        Returns:
            GetResult: A GetResult object containing the results.
        """
        return self._client._peek(self.id, limit)

    def query(
        self,
        query_embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[np.ndarray],
            ]
        ] = None,
        query_texts: Optional[OneOrMany[Document]] = None,
        query_images: Optional[OneOrMany[Image]] = None,
        query_uris: Optional[OneOrMany[URI]] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents", "distances"],
    ) -> QueryResult:
        """Get the n_results nearest neighbor embeddings for provided query_embeddings or query_texts.

        Args:
            query_embeddings: The embeddings to get the closes neighbors of. Optional.
            query_texts: The document texts to get the closes neighbors of. Optional.
            query_images: The images to get the closes neighbors of. Optional.
            n_results: The number of neighbors to return for each query_embedding or query_texts. Optional.
            where: A Where type dict used to filter results by. E.g. `{"$and": ["color" : "red", "price": {"$gte": 4.20}]}`. Optional.
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

        # Users must provide only one of query_embeddings, query_texts, query_images, or query_uris
        if not (
            (query_embeddings is not None)
            ^ (query_texts is not None)
            ^ (query_images is not None)
            ^ (query_uris is not None)
        ):
            raise ValueError(
                "You must provide one of query_embeddings, query_texts, query_images, or query_uris."
            )

        valid_where = validate_where(where) if where else {}
        valid_where_document = (
            validate_where_document(where_document) if where_document else {}
        )
        valid_query_embeddings = (
            validate_embeddings(
                self._normalize_embeddings(
                    maybe_cast_one_to_many_embedding(query_embeddings)
                )
            )
            if query_embeddings is not None
            else None
        )
        valid_query_texts = (
            maybe_cast_one_to_many_document(query_texts)
            if query_texts is not None
            else None
        )
        valid_query_images = (
            maybe_cast_one_to_many_image(query_images)
            if query_images is not None
            else None
        )
        valid_query_uris = (
            maybe_cast_one_to_many_uri(query_uris) if query_uris is not None else None
        )
        valid_include = validate_include(include, allow_distances=True)
        valid_n_results = validate_n_results(n_results)

        # If query_embeddings are not provided, we need to compute them from the inputs
        if valid_query_embeddings is None:
            if query_texts is not None:
                valid_query_embeddings = self._embed(input=valid_query_texts)
            elif query_images is not None:
                valid_query_embeddings = self._embed(input=valid_query_images)
            else:
                if valid_query_uris is None:
                    raise ValueError(
                        "You must provide either query_embeddings, query_texts, query_images, or query_uris."
                    )
                if self._data_loader is None:
                    raise ValueError(
                        "You must set a data loader on the collection if loading from URIs."
                    )
                valid_query_embeddings = self._embed(
                    self._data_loader(valid_query_uris)
                )

        if "data" in include and "uris" not in include:
            valid_include.append("uris")
        query_results = self._client._query(
            collection_id=self.id,
            query_embeddings=valid_query_embeddings,
            n_results=valid_n_results,
            where=valid_where,
            where_document=valid_where_document,
            include=include,
        )

        if (
            "data" in include
            and self._data_loader is not None
            and query_results["uris"] is not None
        ):
            query_results["data"] = [
                self._data_loader(uris) for uris in query_results["uris"]
            ]

        # Remove URIs from the result if they weren't requested
        if "uris" not in include:
            query_results["uris"] = None

        return query_results

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
        if metadata is not None:
            validate_metadata(metadata)
            if "hnsw:space" in metadata:
                raise ValueError(
                    "Changing the distance function of a collection once it is created is not supported currently."
                )

        # Note there is a race condition here where the metadata can be updated
        # but another thread sees the cached local metadata.
        # TODO: fixme
        self._client._modify(id=self.id, new_name=name, new_metadata=metadata)
        if name:
            self._model["name"] = name
        if metadata:
            self._model["metadata"] = metadata

    def update(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
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
        (
            ids,
            embeddings,
            metadatas,
            documents,
            images,
            uris,
        ) = self._validate_embedding_set(
            ids,
            embeddings,
            metadatas,
            documents,
            images,
            uris,
            require_embeddings_or_data=False,
        )

        if embeddings is None:
            if documents is not None:
                embeddings = self._embed(input=documents)
            elif images is not None:
                embeddings = self._embed(input=images)

        self._client._update(self.id, ids, embeddings, metadatas, documents, uris)

    def upsert(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
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
        """Update the embeddings, metadatas or documents for provided ids, or create them if they don't exist.

        Args:
            ids: The ids of the embeddings to update
            embeddings: The embeddings to add. If None, embeddings will be computed based on the documents using the embedding_function set for the Collection. Optional.
            metadatas:  The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
            documents: The documents to associate with the embeddings. Optional.

        Returns:
            None
        """
        (
            ids,
            embeddings,
            metadatas,
            documents,
            images,
            uris,
        ) = self._validate_embedding_set(
            ids, embeddings, metadatas, documents, images, uris
        )

        if embeddings is None:
            if documents is not None:
                embeddings = self._embed(input=documents)
            else:
                embeddings = self._embed(input=images)

        self._client._upsert(
            collection_id=self.id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
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
            where: A Where type dict used to filter the delection by. E.g. `{"$and": ["color" : "red", "price": {"$gte": 4.20}]}`. Optional.
            where_document: A WhereDocument type dict used to filter the deletion by the document content. E.g. `{$contains: {"text": "hello"}}`. Optional.

        Returns:
            None

        Raises:
            ValueError: If you don't provide either ids, where, or where_document
        """
        ids = validate_ids(maybe_cast_one_to_many_ids(ids)) if ids else None
        where = validate_where(where) if where else None
        where_document = (
            validate_where_document(where_document) if where_document else None
        )

        self._client._delete(self.id, ids, where, where_document)
