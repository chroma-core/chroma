from typing import ClassVar, Dict, Any

from chromadb.config import Component
from enum import Enum


class ServerContext(Enum):
    NONE = "None"
    FASTAPI = "FastAPI"


class ProductTelemetryEvent:
    max_batch_size: ClassVar[int] = 1
    batch_size: int

    def __init__(self, batch_size: int = 1):
        self.batch_size = batch_size

    @property
    def properties(self) -> Dict[str, Any]:
        return self.__dict__

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def batch_key(self) -> str:
        return self.name

    def batch(self, other: "ProductTelemetryEvent") -> "ProductTelemetryEvent":
        raise NotImplementedError


class ProductTelemetryClient(Component):
    SERVER_CONTEXT: ServerContext = ServerContext.NONE

    def capture(self, event: ProductTelemetryEvent) -> None:
        pass
