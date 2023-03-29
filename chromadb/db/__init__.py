from abc import ABC, abstractmethod
from typing import Dict, List, Sequence, Optional, Tuple
from uuid import UUID
import numpy.typing as npt
from chromadb.api.types import Embeddings, Documents, IDs, Metadatas, Where, WhereDocument


class DB(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def create_collection(
        self, name: str, metadata: Optional[Dict] = None, get_or_create: bool = False
    ) -> Sequence:
        pass

    @abstractmethod
    def get_collection(self, name: str) -> Sequence:
        pass

    @abstractmethod
    def list_collections(self) -> Sequence:
        pass

    @abstractmethod
    def update_collection(
        self, current_name: str, new_name: Optional[str] = None, new_metadata: Optional[Dict] = None
    ):
        pass

    @abstractmethod
    def delete_collection(self, name: str):
        pass

    @abstractmethod
    def get_collection_uuid_from_name(self, collection_name: str) -> str:
        pass

    @abstractmethod
    def add(
        self,
        collection_uuid: str,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
        ids: List[UUID],
    ) -> List[UUID]:
        pass

    @abstractmethod
    def add_incremental(self, collection_uuid: str, ids: List[UUID], embeddings: Embeddings):
        pass

    @abstractmethod
    def get(
        self,
        where: Where = {},
        collection_name: Optional[str] = None,
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: WhereDocument = {},
        columns: Optional[List[str]] = None,
    ) -> Sequence:
        pass

    @abstractmethod
    def update(
        self,
        collection_uuid: str,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ):
        pass

    @abstractmethod
    def count(self, collection_name: str):
        pass

    @abstractmethod
    def delete(
        self,
        where: Where = {},
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        where_document: WhereDocument = {},
    ) -> List:
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def get_nearest_neighbors(
        self, collection_name, where, embeddings, n_results, where_document
    ) -> Tuple[List[List[UUID]], npt.NDArray]:
        pass

    @abstractmethod
    def get_by_ids(self, uuids, columns=None) -> Sequence:
        pass

    @abstractmethod
    def raw_sql(self, raw_sql):
        pass

    @abstractmethod
    def create_index(self, collection_uuid: str):
        pass

    @abstractmethod
    def persist(self):
        pass
