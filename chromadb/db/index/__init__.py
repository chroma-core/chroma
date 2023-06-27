from abc import ABC, abstractmethod


class Index(ABC):
    @abstractmethod
    def __init__(self, id, settings, metadata):  # type: ignore
        pass

    @abstractmethod
    def delete(self):  # type: ignore
        pass

    @abstractmethod
    def delete_from_index(self, ids):  # type: ignore
        pass

    @abstractmethod
    def add(self, ids, embeddings, update=False):  # type: ignore
        pass

    @abstractmethod
    def get_nearest_neighbors(self, embedding, n_results, ids):  # type: ignore
        pass
