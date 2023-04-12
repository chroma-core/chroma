from typing import TYPE_CHECKING, Optional, cast, List, Dict
from pydantic import BaseModel, PrivateAttr

from chromadb.api.types import (
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
    validate_metadatas,
    validate_where,
    validate_where_document,
)
import logging

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from chromadb.api import API


class Collection(BaseModel):
    name: str
    metadata: Optional[Dict] = None
    _client: "API" = PrivateAttr()
    _embedding_function: Optional[EmbeddingFunction] = PrivateAttr()

    def __init__(
        self,
        client: "API",
        name: str,
        embedding_function: Optional[EmbeddingFunction] = None,
        metadata: Optional[Dict] = None,
    ):

        self._client = client
        if embedding_function is not None:
            self._embedding_function = embedding_function
        else:
            import chromadb.utils.embedding_functions as ef

            logger.warning(
                "No embedding_function provided, using default embedding function: SentenceTransformerEmbeddingFunction"
            )
            self._embedding_function = ef.SentenceTransformerEmbeddingFunction()
        super().__init__(name=name, metadata=metadata)

    def __repr__(self):
        return f"Collection(name={self.name})"

    def count(self) -> int:
        """The total number of embeddings added to the database"""
        return self._client._count(collection_name=self.name)

    def add(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[OneOrMany[Embedding]] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        increment_index: bool = True,
    ):
        """Add embeddings to the data store.
        Args:
            ids: The ids of the embeddings you wish to add
            embedding: The embeddings to add. If None, embeddings will be computed based on the documents using the embedding_function set for the Collection. Optional.
            metadata: The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
            documents: The documents to associate with the embeddings. Optional.
            ids: The ids to associate with the embeddings. Optional.
        """

        ids = validate_ids(maybe_cast_one_to_many(ids))
        embeddings = maybe_cast_one_to_many(embeddings) if embeddings else None
        metadatas = validate_metadatas(maybe_cast_one_to_many(metadatas)) if metadatas else None
        documents = maybe_cast_one_to_many(documents) if documents else None

        # Check that one of embeddings or documents is provided
        if embeddings is None and documents is None:
            raise ValueError("You must provide either embeddings or documents, or both")

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
                raise ValueError("You must provide embeddings or a function to compute them")
            embeddings = self._embedding_function(documents)

        self._client._add(ids, self.name, embeddings, metadatas, documents, increment_index)

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
            where: A Where type dict used to filter results by. E.g. {"color" : "red", "price": 4.20}. Optional.
            limit: The number of documents to return. Optional.
            offset: The offset to start returning results from. Useful for paging results with limit. Optional.
            where_document: A WhereDocument type dict used to filter by the documents. E.g. {$contains: {"text": "hello"}}. Optional.
            include: A list of what to include in the results. Can contain "embeddings", "metadatas", "documents". Ids are always included. Defaults to ["metadatas", "documents"]. Optional.
        """
        where = validate_where(where) if where else None
        where_document = validate_where_document(where_document) if where_document else None
        ids = validate_ids(maybe_cast_one_to_many(ids)) if ids else None
        include = validate_include(include, allow_distances=False)
        return self._client._get(
            self.name,
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
        """
        return self._client._peek(self.name, limit)

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
            n_results: The number of neighbors to return for each query_embedding or query_text. Optional.
            where: A Where type dict used to filter results by. E.g. {"color" : "red", "price": 4.20}. Optional.
            where_document: A WhereDocument type dict used to filter by the documents. E.g. {$contains: {"text": "hello"}}. Optional.
            include: A list of what to include in the results. Can contain "embeddings", "metadatas", "documents", "distances". Ids are always included. Defaults to ["metadatas", "documents", "distances"]. Optional.
        """
        where = validate_where(where) if where else None
        where_document = validate_where_document(where_document) if where_document else None
        query_embeddings = maybe_cast_one_to_many(query_embeddings) if query_embeddings else None
        query_texts = maybe_cast_one_to_many(query_texts) if query_texts else None
        include = validate_include(include, allow_distances=True)

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
                raise ValueError("You must provide embeddings or a function to compute them")
            # We know query texts is not None at this point, cast for the typechecker
            query_embeddings = self._embedding_function(cast(List[Document], query_texts))

        if where is None:
            where = {}

        if where_document is None:
            where_document = {}

        return self._client._query(
            collection_name=self.name,
            query_embeddings=query_embeddings,
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=include,
        )

    def modify(self, name: Optional[str] = None, metadata=None):
        """Modify the collection name or metadata

        Args:
            name: The updated name for the collection. Optional.
            metadata: The updated metadata for the collection. Optional.
        """
        self._client._modify(current_name=self.name, new_name=name, new_metadata=metadata)
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
    ):
        """Update the embeddings, metadatas or documents for provided ids.

        Args:
            ids: The ids of the embeddings to update
            embeddings: The embeddings to add. If None, embeddings will be computed based on the documents using the embedding_function set for the Collection. Optional.
            metadatas:  The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
            documents: The documents to associate with the embeddings. Optional.
        """

        ids = validate_ids(maybe_cast_one_to_many(ids))
        embeddings = maybe_cast_one_to_many(embeddings) if embeddings else None
        metadatas = validate_metadatas(maybe_cast_one_to_many(metadatas)) if metadatas else None
        documents = maybe_cast_one_to_many(documents) if documents else None

        # Must update one of embeddings, metadatas, or documents
        if embeddings is None and documents is None and metadatas is None:
            raise ValueError("You must update at least one of embeddings, documents or metadatas.")

        # Check that one of embeddings or documents is provided
        if embeddings is not None and documents is None:
            raise ValueError("You must provide updated documents with updated embeddings")

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
                raise ValueError("You must provide embeddings or a function to compute them")
            embeddings = self._embedding_function(documents)

        self._client._update(self.name, ids, embeddings, metadatas, documents)

    def delete(
        self,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
    ):
        """Delete the embeddings based on ids and/or a where filter

        Args:
            ids: The ids of the embeddings to delete
            where: A Where type dict used to filter the delection by. E.g. {"color" : "red", "price": 4.20}. Optional.
            where_document: A WhereDocument type dict used to filter the deletion by the document content. E.g. {$contains: {"text": "hello"}}. Optional.
        """
        ids = validate_ids(maybe_cast_one_to_many(ids)) if ids else None
        where = validate_where(where) if where else None
        where_document = validate_where_document(where_document) if where_document else None
        return self._client._delete(self.name, ids, where, where_document)

    def create_index(self):
        self._client.create_index(self.name)
