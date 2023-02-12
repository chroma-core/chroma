from typing import TYPE_CHECKING, Callable, Optional, Sequence, cast, List
from pydantic import BaseModel, PrivateAttr
import json

from chromadb.api.types import (
    Embedding,
    Metadata,
    Document,
    Where,
    Embeddings,
    IDs,
    Metadatas,
    Documents,
    EmbeddingFunction,
    GetResult,
    QueryResult,
    ID,
    OneOrMany,
    maybe_cast_one_to_many,
)

if TYPE_CHECKING:
    from chromadb.api import API


class Collection(BaseModel):
    name: str
    _client: "API" = PrivateAttr()
    _embedding_fn: Optional[EmbeddingFunction] = PrivateAttr()

    def __init__(self, client: "API", name: str, embedding_fn: Optional[EmbeddingFunction] = None):
        self._client = client
        self._embedding_fn = embedding_fn
        super().__init__(name=name)

    def __repr__(self):
        return f"Collection(name={self.name})"

    def count(self) -> int:
        return self._client._count(collection_name=self.name)

    def add(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[OneOrMany[Embedding]] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        increment_index: bool = True,
    ):
        ids = maybe_cast_one_to_many(ids)
        embeddings = maybe_cast_one_to_many(embeddings) if embeddings else None
        metadatas = maybe_cast_one_to_many(metadatas) if metadatas else None
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
            if self._embedding_fn is None:
                raise ValueError("You must provide embeddings or a function to compute them")
            embeddings = self._embedding_fn(documents)

        self._client._add(ids, self.name, embeddings, metadatas, documents, increment_index)

    def get(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        where: Optional[Where] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> GetResult:
        ids = maybe_cast_one_to_many(ids) if ids else None
        return self._client._get(self.name, ids, where, sort, limit, offset)

    def peek(self, limit: int = 10) -> GetResult:
        return self._client._peek(self.name, limit)

    def query(
        self,
        query_embeddings: Optional[OneOrMany[Embedding]] = None,
        query_texts: Optional[OneOrMany[Document]] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
    ) -> QueryResult:
        query_embeddings = maybe_cast_one_to_many(query_embeddings) if query_embeddings else None
        query_texts = maybe_cast_one_to_many(query_texts) if query_texts else None

        # If neither query_embeddings nor query_texts are provided, or both are provided, raise an error
        if (query_embeddings is None and query_texts is None) or (
            query_embeddings is not None and query_texts is not None
        ):
            raise ValueError(
                "You must provide either query embeddings or query texts, but not both"
            )

        # If query_embeddings are not provided, we need to compute them from the query_texts
        if query_embeddings is None:
            if self._embedding_fn is None:
                raise ValueError("You must provide embeddings or a function to compute them")
            # We know query texts is not None at this point, cast for the typechecker
            query_embeddings = self._embedding_fn(cast(List[Document], query_texts))

        if where is None:
            where = {}

        return self._client._query(
            collection_name=self.name,
            query_embeddings=query_embeddings,
            n_results=n_results,
            where=where,
        )

    def modify(self, name: Optional[str] = None, metadata=None):
        self._client._modify(current_name=self.name, new_name=name, new_metadata=metadata)
        if name:
            self.name = name

    def update(
        self,
        ids: IDs,
        embeddings: Optional[Embeddings],
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
    ):
        raise NotImplementedError()

    def upsert(
        self,
        ids: IDs,
        embeddings: Optional[Embeddings],
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
    ):
        raise NotImplementedError()

    def delete(self, ids=None, where=None):
        return self._client._delete(self.name, ids, where)

    def create_index(self):
        self._client.create_index(self.name)
