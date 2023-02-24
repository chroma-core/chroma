from abc import ABC, abstractmethod
from chromadb.config import Settings
from typing import List


class Index(ABC):
    @abstractmethod
    def __init__(self, settings: Settings):
        pass

    @abstractmethod
    def delete(self, collection_name: str):
        pass

    @abstractmethod
    def delete_from_index(self, collection_name: str, uuids: List[str]):
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def run(self, collection_name: str, uuids: List[str], embeddings):
        pass

    @abstractmethod
    def has_index(self, collection_name: str):
        pass

    @abstractmethod
    def get_nearest_neighbors(
        self, collection_name: str, embedding, n_results: int, ids: List[str]
    ):
        pass
