from abc import abstractmethod


class Database:
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def add(self, model_space, embedding, input_uri, dataset=None, custom_quality_score=None, inference_class=None, label_class=None):
        pass

    @abstractmethod
    def count(self, model_space=None):
        pass

    @abstractmethod
    def fetch(self, where_filter={}, sort=None, limit=None):
        pass

    @abstractmethod
    def get_by_ids(self, ids):
        pass

    @abstractmethod
    def reset(self):
        pass
