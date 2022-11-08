from abc import abstractmethod

class Telemetry():
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def capture(self, event, properties=None):
        pass