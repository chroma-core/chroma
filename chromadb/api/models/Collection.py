from typing import TYPE_CHECKING, Callable, Optional, Sequence
from pydantic import BaseModel, PrivateAttr
import json

from chromadb.api.types import (
    Where,
    Embeddings,
    IDs,
    Metadatas,
    Documents,
    EmbeddingFunction,
    GetResult,
    QueryResult,
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
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ):
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
        if embeddings is None:
            if self._embedding_fn is None:
                raise ValueError("You must provide embeddings or a function to compute them")
            embeddings = self._embedding_fn(documents)

        self._client._add(ids, self.name, embeddings, metadatas, documents, increment_index)
        # NIT: return something?
        return

    def get(
        self,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> GetResult:
        return self._client._get(self.name, ids, where, sort, limit, offset)

    def peek(self, limit: int = 10) -> GetResult:
        return self._client._peek(self.name, limit)

    def query(
        self,
        query_embeddings: Optional[Embeddings] = None,
        query_texts: Optional[Documents] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
    ) -> QueryResult:

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
            query_embeddings = self._embedding_fn(query_texts)

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
