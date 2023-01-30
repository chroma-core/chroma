from abc import ABC, abstractmethod

class DB(ABC):

    @abstractmethod
    def __init__(self):
        pass


    @abstractmethod
    def add(self,
            model_space: str,
            embedding,
            input_uri,
            dataset=None,
            custom_quality_score=None,
            metadata=None
            ):
        pass


    @abstractmethod
    def fetch(self, where, sort, limit, offset):
        pass

    @abstractmethod
    def count(self, model_space=None):
        pass

    @abstractmethod
    def delete(self, where):
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
    def create_index(self, model_space):
        pass


    @abstractmethod
    def has_index(self, model_space):
        pass
