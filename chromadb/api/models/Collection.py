from typing import TYPE_CHECKING, Optional, Union, Sequence

from chromadb.api.types import (
    NearestNeighborsResult,
    Where,
    Embeddings,
    IDs,
    Metadatas,
    Documents,
    Item,
)

if TYPE_CHECKING:
    from chromadb.api import API

# collection class
class Collection:
    def __init__(self, client: "API", name: str):
        self.client = client
        self.name = name

    def __repr__(self):
        return f"Collection(name={self.name})"

    def __dict__(self):
        return {
            "name": self.name,
        }

    def count(self) -> int:
        return self.client._count(collection_name=self.name)

    def add(
        self,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ):
        self.client._add(ids, self.name, embeddings, metadatas, documents)

    def get(
        self,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[Item]:
        return self.client._get(self.name, ids, where, sort, limit, offset)

    def peek(self, limit: int = 10) -> list[Item]:
        return self.client._peek(self.name, limit)

    def query(
        self, query_embeddings: Embeddings, n_results: int = 10, where: Where = {}
    ) -> Sequence[NearestNeighborsResult]:
        return self.client._query(
            collection_name=self.name,
            query_embeddings=query_embeddings,
            n_results=n_results,
            where=where,
        )

    def modify(self, name: Optional[str] = None, metadata=None):
        self.client._modify(current_name=self.name, new_name=name, new_metadata=metadata)
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
        return self.client._delete(self.name, ids, where)
