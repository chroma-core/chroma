from abc import ABC, abstractmethod

class Index(ABC):

    @abstractmethod
    def __init__(self, settings):
        pass


    @abstractmethod
    def delete(self, collection_name):
        pass


    @abstractmethod
    def delete_from_index(self, collection_name, uuids):
        pass


    @abstractmethod
    def reset(self):
        pass


    @abstractmethod
    def run(self, collection_name, uuids, embeddings):
        pass


    @abstractmethod
    def has_index(self, collection_name):
        pass


    @abstractmethod
    def get_nearest_neighbors(self, collection_name, embedding, n_results, ids):
        pass
