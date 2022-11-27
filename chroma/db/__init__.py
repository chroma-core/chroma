from abc import ABC, abstractmethod

class DB(ABC):

    @abstractmethod
    def __init__(self):
        pass


    @abstractmethod
    def add(self, model_space, embedding, input_uri, dataset=None, custom_quality_score=None, inference_class=None, label_class=None):
        pass


    @abstractmethod
    def fetch(self, where, sort, limit, offset, columnar):
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


    @abstractmethod
    def count_results(self, model_space):
        pass


    @abstractmethod
    def return_results(self, model_space, n_results):
        pass


    @abstractmethod
    def delete_results(self, model_space):
        pass


    @abstractmethod
    def add_results(self, model_space, uuids, quality_scores):
        pass


    @abstractmethod
    def get_col_pos(self, col_name):
        pass
