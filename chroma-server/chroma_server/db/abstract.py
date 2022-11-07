from abc import abstractmethod

# TODO: update this to match the clickhouse implementation
class Database():
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def add_batch(self, batch):
        pass

    @abstractmethod
    def fetch(self, query):
        pass