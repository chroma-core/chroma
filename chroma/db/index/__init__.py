from abc import ABC, abstractmethod

class Index(ABC):

    @abstractmethod
    def __init__(self, settings):
        pass


    @abstractmethod
    def delete(self, model_space):
        pass


    @abstractmethod
    def delete_from_index(self, model_space, uuids):
        pass


    @abstractmethod
    def reset(self):
        pass


    @abstractmethod
    def run(self, model_space, uuids, embeddings):
        pass


    @abstractmethod
    def has_index(self, model_space):
        pass


    @abstractmethod
    def get_nearest_neighbors(self, model_space, embedding, n_results, ids):
        pass
