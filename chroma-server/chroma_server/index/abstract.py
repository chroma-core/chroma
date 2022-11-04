from abc import abstractmethod

class Index():
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def run(self, batch):
        pass

    @abstractmethod
    def fetch(self, query):
        pass

    @abstractmethod
    def delete_batch(self, batch):
        pass

    @abstractmethod
    def persist(self):
        pass

    @abstractmethod
    def load(self):
        pass