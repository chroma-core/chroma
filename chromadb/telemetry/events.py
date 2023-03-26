from dataclasses import dataclass
from typing import ClassVar
from chromadb.telemetry import TelemetryEvent


@dataclass
class ClientStartEvent(TelemetryEvent):
    name: ClassVar[str] = "client_start"


@dataclass
class ServerStartEvent(TelemetryEvent):
    name: ClassVar[str] = "server_start"


@dataclass
class CollectionAddEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_add"
    collection_uuid: str
    add_amount: int


@dataclass
class CollectionDeleteEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_delete"
    collection_uuid: str
    delete_amount: int
