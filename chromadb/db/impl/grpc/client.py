from typing import List, Optional, Sequence, Tuple, Union, cast
from uuid import UUID
from overrides import overrides
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, System
from chromadb.db.base import NotFoundError, UniqueConstraintError
from chromadb.db.system import SysDB
from chromadb.proto.convert import (
    from_proto_collection,
    from_proto_segment,
    to_proto_update_metadata,
    to_proto_segment,
    to_proto_segment_scope,
)
from chromadb.proto.coordinator_pb2 import (
    CreateCollectionRequest,
    CreateDatabaseRequest,
    CreateSegmentRequest,
    CreateTenantRequest,
    DeleteCollectionRequest,
    DeleteSegmentRequest,
    GetCollectionsRequest,
    GetCollectionsResponse,
    GetDatabaseRequest,
    GetSegmentsRequest,
    GetTenantRequest,
    UpdateCollectionRequest,
    UpdateSegmentRequest,
)
from chromadb.proto.coordinator_pb2_grpc import SysDBStub
from chromadb.telemetry.opentelemetry import OpenTelemetryClient
from chromadb.telemetry.opentelemetry.grpc import OtelInterceptor
from chromadb.types import (
    Collection,
    Database,
    Metadata,
    OptionalArgument,
    Segment,
    SegmentScope,
    Tenant,
    Unspecified,
    UpdateMetadata,
)
from google.protobuf.empty_pb2 import Empty
import grpc


class GrpcSysDB(SysDB):
    """A gRPC implementation of the SysDB. In the distributed system, the SysDB is also
    called the 'Coordinator'. This implementation is used by Chroma frontend servers
    to call a remote SysDB (Coordinator) service."""

    _sys_db_stub: SysDBStub
    _channel: grpc.Channel
    _coordinator_url: str
    _coordinator_port: int

    def __init__(self, system: System):
        self._coordinator_url = system.settings.require("chroma_coordinator_host")
        # TODO: break out coordinator_port into a separate setting?
        self._coordinator_port = system.settings.require("chroma_server_grpc_port")
        return super().__init__(system)

    @overrides
    def start(self) -> None:
        # TODO: add retry policy here
        self._channel = grpc.insecure_channel(
            f"{self._coordinator_url}:{self._coordinator_port}"
        )
        # interceptors = [OtelInterceptor()]
        # self._channel = grpc.intercept_channel(self._channel, *interceptors)
        self._sys_db_stub = SysDBStub(self._channel)  # type: ignore
        return super().start()

    @overrides
    def stop(self) -> None:
        self._channel.close()
        return super().stop()

    @overrides
    def reset_state(self) -> None:
        self._sys_db_stub.ResetState(Empty())
        return super().reset_state()

    @overrides
    def create_database(
        self, id: UUID, name: str, tenant: str = DEFAULT_TENANT
    ) -> None:
        request = CreateDatabaseRequest(id=id.hex, name=name, tenant=tenant)
        response = self._sys_db_stub.CreateDatabase(request)
        if response.status.code == 409:
            raise UniqueConstraintError()

    @overrides
    def get_database(self, name: str, tenant: str = DEFAULT_TENANT) -> Database:
        request = GetDatabaseRequest(name=name, tenant=tenant)
        response = self._sys_db_stub.GetDatabase(request)
        if response.status.code == 404:
            raise NotFoundError()
        return Database(
            id=UUID(hex=response.database.id),
            name=response.database.name,
            tenant=response.database.tenant,
        )

    @overrides
    def create_tenant(self, name: str) -> None:
        request = CreateTenantRequest(name=name)
        response = self._sys_db_stub.CreateTenant(request)
        if response.status.code == 409:
            raise UniqueConstraintError()

    @overrides
    def get_tenant(self, name: str) -> Tenant:
        request = GetTenantRequest(name=name)
        response = self._sys_db_stub.GetTenant(request)
        if response.status.code == 404:
            raise NotFoundError()
        return Tenant(
            name=response.tenant.name,
        )

    @overrides
    def create_segment(self, segment: Segment) -> None:
        proto_segment = to_proto_segment(segment)
        request = CreateSegmentRequest(
            segment=proto_segment,
        )
        response = self._sys_db_stub.CreateSegment(request)
        if response.status.code == 409:
            raise UniqueConstraintError()

    @overrides
    def delete_segment(self, id: UUID) -> None:
        request = DeleteSegmentRequest(
            id=id.hex,
        )
        response = self._sys_db_stub.DeleteSegment(request)
        if response.status.code == 404:
            raise NotFoundError()

    @overrides
    def get_segments(
        self,
        id: Optional[UUID] = None,
        type: Optional[str] = None,
        scope: Optional[SegmentScope] = None,
        topic: Optional[str] = None,
        collection: Optional[UUID] = None,
    ) -> Sequence[Segment]:
        request = GetSegmentsRequest(
            id=id.hex if id else None,
            type=type,
            scope=to_proto_segment_scope(scope) if scope else None,
            topic=topic,
            collection=collection.hex if collection else None,
        )
        response = self._sys_db_stub.GetSegments(request)
        results: List[Segment] = []
        for proto_segment in response.segments:
            segment = from_proto_segment(proto_segment)
            results.append(segment)
        return results

    @overrides
    def update_segment(
        self,
        id: UUID,
        topic: OptionalArgument[Optional[str]] = Unspecified(),
        collection: OptionalArgument[Optional[UUID]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        write_topic = None
        if topic != Unspecified():
            write_topic = cast(Union[str, None], topic)

        write_collection = None
        if collection != Unspecified():
            write_collection = cast(Union[UUID, None], collection)

        write_metadata = None
        if metadata != Unspecified():
            write_metadata = cast(Union[UpdateMetadata, None], metadata)

        request = UpdateSegmentRequest(
            id=id.hex,
            topic=write_topic,
            collection=write_collection.hex if write_collection else None,
            metadata=to_proto_update_metadata(write_metadata)
            if write_metadata
            else None,
        )

        if topic is None:
            request.ClearField("topic")
            request.reset_topic = True

        if collection is None:
            request.ClearField("collection")
            request.reset_collection = True

        if metadata is None:
            request.ClearField("metadata")
            request.reset_metadata = True

        self._sys_db_stub.UpdateSegment(request)

    @overrides
    def create_collection(
        self,
        id: UUID,
        name: str,
        metadata: Optional[Metadata] = None,
        dimension: Optional[int] = None,
        get_or_create: bool = False,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Tuple[Collection, bool]:
        request = CreateCollectionRequest(
            id=id.hex,
            name=name,
            metadata=to_proto_update_metadata(metadata) if metadata else None,
            dimension=dimension,
            get_or_create=get_or_create,
            tenant=tenant,
            database=database,
        )
        response = self._sys_db_stub.CreateCollection(request)
        if response.status.code == 409:
            raise UniqueConstraintError()
        collection = from_proto_collection(response.collection)
        return collection, response.created

    @overrides
    def delete_collection(
        self, id: UUID, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> None:
        request = DeleteCollectionRequest(
            id=id.hex,
            tenant=tenant,
            database=database,
        )
        response = self._sys_db_stub.DeleteCollection(request)
        if response.status.code == 404:
            raise NotFoundError()

    @overrides
    def get_collections(
        self,
        id: Optional[UUID] = None,
        topic: Optional[str] = None,
        name: Optional[str] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[Collection]:
        # TODO: implement limit and offset in the gRPC service
        request = GetCollectionsRequest(
            id=id.hex if id else None,
            topic=topic,
            name=name,
            tenant=tenant,
            database=database,
        )
        response: GetCollectionsResponse = self._sys_db_stub.GetCollections(request)
        results: List[Collection] = []
        for collection in response.collections:
            results.append(from_proto_collection(collection))
        return results

    @overrides
    def update_collection(
        self,
        id: UUID,
        topic: OptionalArgument[str] = Unspecified(),
        name: OptionalArgument[str] = Unspecified(),
        dimension: OptionalArgument[Optional[int]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        write_topic = None
        if topic != Unspecified():
            write_topic = cast(str, topic)

        write_name = None
        if name != Unspecified():
            write_name = cast(str, name)

        write_dimension = None
        if dimension != Unspecified():
            write_dimension = cast(Union[int, None], dimension)

        write_metadata = None
        if metadata != Unspecified():
            write_metadata = cast(Union[UpdateMetadata, None], metadata)

        request = UpdateCollectionRequest(
            id=id.hex,
            topic=write_topic,
            name=write_name,
            dimension=write_dimension,
            metadata=to_proto_update_metadata(write_metadata)
            if write_metadata
            else None,
        )
        if metadata is None:
            request.ClearField("metadata")
            request.reset_metadata = True

        response = self._sys_db_stub.UpdateCollection(request)
        if response.status.code == 404:
            raise NotFoundError()

    def reset_and_wait_for_ready(self) -> None:
        self._sys_db_stub.ResetState(Empty(), wait_for_ready=True)
