from typing import TYPE_CHECKING, Optional, Tuple, cast, List
from pydantic import BaseModel, PrivateAttr

from uuid import UUID
import chromadb.utils.embedding_functions as ef

from chromadb.api.types import (
    CollectionMetadata,
    Embedding,
    Include,
    Metadata,
    Document,
    Where,
    IDs,
    EmbeddingFunction,
    GetResult,
    QueryResult,
    ID,
    OneOrMany,
    WhereDocument,
    maybe_cast_one_to_many,
    validate_ids,
    validate_include,
    validate_metadata,
    validate_metadatas,
    validate_where,
    validate_where_document,
    validate_n_results,
    validate_embeddings,
)
import logging

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from chromadb.api import API


class Collection(BaseModel):
    name: str
    id: UUID
    metadata: Optional[CollectionMetadata] = None
    _client: "API" = PrivateAttr()
    _embedding_function: Optional[EmbeddingFunction] = PrivateAttr()

    def __init__(
        self,
        client: "API",
        name: str,
        id: UUID,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
        metadata: Optional[CollectionMetadata] = None,
    ):
        super().__init__(name=name, metadata=metadata, id=id)
        self._client = client
        self._embedding_function = embedding_function

    def __repr__(self) -> str:
        return f"Collection(name={self.name})"

    def count(self) -> int:
        """The total number of embeddings added to the database

        Returns:
            int: The total number of embeddings added to the database

        """
        return self._client._count(collection_id=self.id)

    def add(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[OneOrMany[Embedding]] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
    ) -> None:
        """Add embeddings to the data store.
        Args:
            ids: The ids of the embeddings you wish to add
            embeddings: The embeddings to add. If None, embeddings will be computed based on the documents using the embedding_function set for the Collection. Optional.
            metadatas: The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
            documents: The documents to associate with the embeddings. Optional.

        Returns:
            None

        Raises:
            ValueError: If you don't provide either embeddings or documents
            ValueError: If the length of ids, embeddings, metadatas, or documents don't match
            ValueError: If you don't provide an embedding function and don't provide embeddings
            ValueError: If you provide both embeddings and documents
            ValueError: If you provide an id that already exists

        """

        ids, embeddings, metadatas, documents = self._validate_embedding_set(
            ids, embeddings, metadatas, documents
        )

        self._client._add(ids, self.id, embeddings, metadatas, documents)

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
        where = validate_where(where) if where else None
        where_document = (
            validate_where_document(where_document) if where_document else None
        )
        ids = validate_ids(maybe_cast_one_to_many(ids)) if ids else None
        include = validate_include(include, allow_distances=False)
        return self._client._get(
            self.id,
            ids,
            where,
            None,
            limit,
            offset,
            where_document=where_document,
            include=include,
        )

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
        query_embeddings: Optional[OneOrMany[Embedding]] = None,
        query_texts: Optional[OneOrMany[Document]] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents", "distances"],
    ) -> QueryResult:
        """Get the n_results nearest neighbor embeddings for provided query_embeddings or query_texts.

        Args:
            query_embeddings: The embeddings to get the closes neighbors of. Optional.
            query_texts: The document texts to get the closes neighbors of. Optional.
            n_results: The number of neighbors to return for each query_embedding or query_texts. Optional.
            where: A Where type dict used to filter results by. E.g. `{"$and": ["color" : "red", "price": {"$gte": 4.20}]}`. Optional.
            where_document: A WhereDocument type dict used to filter by the documents. E.g. `{$contains: {"text": "hello"}}`. Optional.
            include: A list of what to include in the results. Can contain `"embeddings"`, `"metadatas"`, `"documents"`, `"distances"`. Ids are always included. Defaults to `["metadatas", "documents", "distances"]`. Optional.

        Returns:
            QueryResult: A QueryResult object containing the results.

        Raises:
            ValueError: If you don't provide either query_embeddings or query_texts
            ValueError: If you provide both query_embeddings and query_texts

        """
        where = validate_where(where) if where else None
        where_document = (
            validate_where_document(where_document) if where_document else None
        )
        query_embeddings = (
            validate_embeddings(maybe_cast_one_to_many(query_embeddings))
            if query_embeddings is not None
            else None
        )
        query_texts = (
            maybe_cast_one_to_many(query_texts) if query_texts is not None else None
        )
        include = validate_include(include, allow_distances=True)
        n_results = validate_n_results(n_results)

        # If neither query_embeddings nor query_texts are provided, or both are provided, raise an error
        if (query_embeddings is None and query_texts is None) or (
            query_embeddings is not None and query_texts is not None
        ):
            raise ValueError(
                "You must provide either query embeddings or query texts, but not both"
            )

        # If query_embeddings are not provided, we need to compute them from the query_texts
        if query_embeddings is None:
            if self._embedding_function is None:
                raise ValueError(
                    "You must provide embeddings or a function to compute them"
                )
            # We know query texts is not None at this point, cast for the typechecker
            query_embeddings = self._embedding_function(
                cast(List[Document], query_texts)
            )

        if where is None:
            where = {}

        if where_document is None:
            where_document = {}

        return self._client._query(
            collection_id=self.id,
            query_embeddings=query_embeddings,
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=include,
        )

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

        self._client._modify(id=self.id, new_name=name, new_metadata=metadata)
        if name:
            self.name = name
        if metadata:
            self.metadata = metadata

    def update(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[OneOrMany[Embedding]] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
    ) -> None:
        """Update the embeddings, metadatas or documents for provided ids.

        Args:
            ids: The ids of the embeddings to update
            embeddings: The embeddings to add. If None, embeddings will be computed based on the documents using the embedding_function set for the Collection. Optional.
            metadatas:  The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
            documents: The documents to associate with the embeddings. Optional.

        Returns:
            None
        """

        ids, embeddings, metadatas, documents = self._validate_embedding_set(
            ids, embeddings, metadatas, documents, require_embeddings_or_documents=False
        )

        self._client._update(self.id, ids, embeddings, metadatas, documents)

    def upsert(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[OneOrMany[Embedding]] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
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

        ids, embeddings, metadatas, documents = self._validate_embedding_set(
            ids, embeddings, metadatas, documents
        )

        self._client._upsert(
            collection_id=self.id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
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
        ids = validate_ids(maybe_cast_one_to_many(ids)) if ids else None
        where = validate_where(where) if where else None
        where_document = (
            validate_where_document(where_document) if where_document else None
        )

        self._client._delete(self.id, ids, where, where_document)

    def _validate_embedding_set(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[OneOrMany[Embedding]],
        metadatas: Optional[OneOrMany[Metadata]],
        documents: Optional[OneOrMany[Document]],
        require_embeddings_or_documents: bool = True,
    ) -> Tuple[
        IDs,
        List[Embedding],
        Optional[List[Metadata]],
        Optional[List[Document]],
    ]:
        ids = validate_ids(maybe_cast_one_to_many(ids))
        embeddings = (
            validate_embeddings(maybe_cast_one_to_many(embeddings))
            if embeddings is not None
            else None
        )
        metadatas = (
            validate_metadatas(maybe_cast_one_to_many(metadatas))
            if metadatas is not None
            else None
        )
        documents = maybe_cast_one_to_many(documents) if documents is not None else None

        # Check that one of embeddings or documents is provided
        if require_embeddings_or_documents:
            if embeddings is None and documents is None:
                raise ValueError(
                    "You must provide either embeddings or documents, or both"
                )

        # Check that, if they're provided, the lengths of the arrays match the length of ids
        if embeddings is not None and len(embeddings) != len(ids):
            raise ValueError(
                f"Number of embeddings {len(embeddings)} must match number of ids {len(ids)}"
            )
        if metadatas is not None and len(metadatas) != len(ids):
            raise ValueError(
                f"Number of metadatas {len(metadatas)} must match number of ids {len(ids)}"
            )
        if documents is not None and len(documents) != len(ids):
            raise ValueError(
                f"Number of documents {len(documents)} must match number of ids {len(ids)}"
            )

        # If document embeddings are not provided, we need to compute them
        if embeddings is None and documents is not None:
            if self._embedding_function is None:
                raise ValueError(
                    "You must provide embeddings or a function to compute them"
                )
            embeddings = self._embedding_function(documents)

        # if embeddings is None:
        #     raise ValueError(
        #         "Something went wrong. Embeddings should be computed at this point"
        #     )

        return ids, embeddings, metadatas, documents  # type: ignore
