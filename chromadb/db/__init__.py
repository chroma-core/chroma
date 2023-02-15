from abc import ABC, abstractmethod
from typing import Sequence


class DB(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def create_collection(self, name, metadata=None):
        pass

    @abstractmethod
    def get_collection(self, collection_uuid):
        pass

    @abstractmethod
    def list_collections(self) -> Sequence[Sequence[str]]:
        pass

    @abstractmethod
    def update_collection(self, collection_uuid, name=None, metadata=None):
        pass

    @abstractmethod
    def delete_collection(self, collection_uuid):
        pass

    @abstractmethod
    def get_collection_uuid_from_name(self, collection_name: str) -> str:
        pass

    @abstractmethod
    def add(
        self,
        collection_name: str,
        embedding,
        input_uri,
        dataset=None,
        custom_quality_score=None,
        metadata=None,
    ):
        pass

    @abstractmethod
    def get(
        self,
        where={},
        collection_name=None,
        collection_uuid=None,
        ids=None,
        sort=None,
        limit=None,
        offset=None,
        where_document={},
    ):
        pass

    @abstractmethod
    def count(self, collection_name=None):
        pass

    @abstractmethod
    def delete(self, ids, where):
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def get_nearest_neighbors(self, where, embedding, n_results):
        pass

    @abstractmethod
    def get_by_ids(self, uuids):
        pass

    @abstractmethod
    def raw_sql(self, raw_sql):
        pass

    @abstractmethod
    def create_index(self, collection_name):
        pass

    @abstractmethod
    def has_index(self, collection_name):
        pass
