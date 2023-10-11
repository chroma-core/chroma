from typing import Optional, Sequence
from uuid import UUID
from overrides import overrides
from chromadb.config import System
from chromadb.db.system import SysDB
from chromadb.proto.coordinator_pb2_grpc import SysDBStub
from chromadb.types import (
    Collection,
    OptionalArgument,
    Segment,
    SegmentScope,
    Unspecified,
    UpdateMetadata,
)
import grpc


class GrpcSysDB(SysDB):
    """A gRPC implementation of the SysDB. In the distributed system, the SysDB is also
    called the 'Coordinator'. This implementation is used by Chroma frontend servers
    to call a remote SysDB (Coordinator) service."""

    _sys_db_stub: SysDBStub
    _coordinator_url: str
    _coordinator_port: int

    def __init__(self, system: System):
        self._coordinator_url = system.settings.require("coordinator_host")
        # TODO: break out coordinator_port into a separate setting?
        self._coordinator_port = system.settings.require("chroma_server_grpc_port")

    @overrides
    def start(self) -> None:
        channel = grpc.insecure_channel(self._coordinator_url)
        self._sys_db_stub = SysDBStub(channel)  # type: ignore
        return super().start()

    @overrides
    def stop(self) -> None:
        return super().stop()

    @overrides
    def reset_state(self) -> None:
        # TODO - remote service should be able to reset state for testing
        return super().reset_state()

    @overrides
    def create_segment(self, segment: Segment) -> None:
        return super().create_segment(segment)

    @overrides
    def delete_segment(self, id: UUID) -> None:
        raise NotImplementedError()

    @overrides
    def get_segments(
        self,
        id: Optional[UUID] = None,
        type: Optional[str] = None,
        scope: Optional[SegmentScope] = None,
        topic: Optional[str] = None,
        collection: Optional[UUID] = None,
    ) -> Sequence[Segment]:
        raise NotImplementedError()

    @overrides
    def update_segment(
        self,
        id: UUID,
        topic: OptionalArgument[Optional[str]] = Unspecified(),
        collection: OptionalArgument[Optional[UUID]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        raise NotImplementedError()

    @overrides
    def create_collection(self, collection: Collection) -> None:
        raise NotImplementedError()

    @overrides
    def delete_collection(self, id: UUID) -> None:
        raise NotImplementedError()

    @overrides
    def get_collections(
        self,
        id: Optional[UUID] = None,
        topic: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Sequence[Collection]:
        raise NotImplementedError()

    @overrides
    def update_collection(
        self,
        id: UUID,
        topic: OptionalArgument[str] = Unspecified(),
        name: OptionalArgument[str] = Unspecified(),
        dimension: OptionalArgument[Optional[int]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        raise NotImplementedError()
