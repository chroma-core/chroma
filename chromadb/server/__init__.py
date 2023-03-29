from abc import ABC, abstractmethod


class Server(ABC):

    @abstractmethod
    def __init__(self, settings):
        pass
