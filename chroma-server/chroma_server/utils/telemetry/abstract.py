from abc import abstractmethod

class Telemetry():
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def add_batch(self, batch):
        pass