from abc import ABC, abstractmethod


class Index(ABC):
    @abstractmethod
    def __init__(self, id, settings, metadata):
        pass

    @abstractmethod
    def delete(self):
        pass

    @abstractmethod
    def delete_from_index(self, ids):
        pass

    @abstractmethod
    def add(self, ids, embeddings, update=False):
        pass

    @abstractmethod
    def get_nearest_neighbors(self, embedding, n_results, ids):
        pass
