from abc import ABC, abstractmethod

class Telemetry(ABC):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def capture(self, event, properties=None):
        pass
