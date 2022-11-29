from abc import ABC, abstractmethod

#from chroma.utils.error_reporting import init_error_reporting
from chroma.server.utils.telemetry.capture import Capture

class Server(ABC):

    def __init__(self, settings):
        self._chroma_telemetry = Capture()
        self._chroma_telemetry.capture('server-start')
 #       init_error_reporting()
