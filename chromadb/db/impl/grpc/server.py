from concurrent import futures
from typing import Any, Dict, cast
from uuid import UUID
from overrides import overrides
from chromadb.config import Component, System
from chromadb.proto.convert import (
    from_proto_collection,
    from_proto_update_metadata,
    from_proto_segment,
    from_proto_segment_scope,
    to_proto_collection,
    to_proto_segment,
)
import chromadb.proto.chroma_pb2 as proto
from chromadb.proto.coordinator_pb2 import (
    CreateCollectionRequest,
    CreateCollectionResponse,
    CreateSegmentRequest,
    DeleteCollectionRequest,
    DeleteSegmentRequest,
    GetCollectionsRequest,
    GetCollectionsResponse,
    GetSegmentsRequest,
    GetSegmentsResponse,
    UpdateCollectionRequest,
    UpdateSegmentRequest,
)
from chromadb.proto.coordinator_pb2_grpc import (
    SysDBServicer,
    add_SysDBServicer_to_server,
)
import grpc
from google.protobuf.empty_pb2 import Empty
from chromadb.types import Collection, Metadata, Segment


class GrpcMockSysDB(SysDBServicer, Component):
    """A mock sysdb implementation that can be used for testing the grpc client. It stores
    state in simple python data structures instead of a database."""

    _server: grpc.Server
    _segments: Dict[str, Segment] = {}
    _collections: Dict[str, Collection] = {}

    def __init__(self, system: System):
        return super().__init__(system)

    @overrides
    def start(self) -> None:
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_SysDBServicer_to_server(self, self._server)  # type: ignore
        self._server.add_insecure_port("[::]:50051")  # TODO: make port configurable
        self._server.start()
        return super().start()

    @overrides
    def stop(self) -> None:
        self._server.stop(0)
        return super().stop()

    @overrides
    def reset_state(self) -> None:
        self._segments = {}
        self._collections = {}
        return super().reset_state()

    # We are forced to use check_signature=False because the generated proto code
    # does not have type annotations for the request and response objects.
    # TODO: investigate generating types for the request and response objects
    @overrides(check_signature=False)
    def CreateSegment(
        self, request: CreateSegmentRequest, context: grpc.ServicerContext
    ) -> proto.ChromaResponse:
        segment = from_proto_segment(request.segment)
        if segment["id"].hex in self._segments:
            return proto.ChromaResponse(
                status=proto.Status(
                    code=409, reason=f"Segment {segment['id']} already exists"
                )
            )
        self._segments[segment["id"].hex] = segment
        return proto.ChromaResponse(
            status=proto.Status(code=200)
        )  # TODO: how are these codes used?

    @overrides(check_signature=False)
    def DeleteSegment(
        self, request: DeleteSegmentRequest, context: grpc.ServicerContext
    ) -> proto.ChromaResponse:
        id_to_delete = request.id
        if id_to_delete in self._segments:
            del self._segments[id_to_delete]
            return proto.ChromaResponse(status=proto.Status(code=200))
        else:
            return proto.ChromaResponse(
                status=proto.Status(
                    code=404, reason=f"Segment {id_to_delete} not found"
                )
            )

    @overrides(check_signature=False)
    def GetSegments(
        self, request: GetSegmentsRequest, context: grpc.ServicerContext
    ) -> GetSegmentsResponse:
        target_id = UUID(hex=request.id) if request.HasField("id") else None
        target_type = request.type if request.HasField("type") else None
        target_scope = (
            from_proto_segment_scope(request.scope)
            if request.HasField("scope")
            else None
        )
        target_topic = request.topic if request.HasField("topic") else None
        target_collection = (
            UUID(hex=request.collection) if request.HasField("collection") else None
        )

        found_segments = []
        for segment in self._segments.values():
            if target_id and segment["id"] != target_id:
                continue
            if target_type and segment["type"] != target_type:
                continue
            if target_scope and segment["scope"] != target_scope:
                continue
            if target_topic and segment["topic"] != target_topic:
                continue
            if target_collection and segment["collection"] != target_collection:
                continue
            found_segments.append(segment)
        return GetSegmentsResponse(
            segments=[to_proto_segment(segment) for segment in found_segments]
        )

    @overrides(check_signature=False)
    def UpdateSegment(
        self, request: UpdateSegmentRequest, context: grpc.ServicerContext
    ) -> proto.ChromaResponse:
        id_to_update = UUID(request.id)
        if id_to_update.hex not in self._segments:
            return proto.ChromaResponse(
                status=proto.Status(
                    code=404, reason=f"Segment {id_to_update} not found"
                )
            )
        else:
            segment = self._segments[id_to_update.hex]
            if request.HasField("topic"):
                segment["topic"] = request.topic
            if request.HasField("reset_topic") and request.reset_topic:
                segment["topic"] = None
            if request.HasField("collection"):
                segment["collection"] = UUID(hex=request.collection)
            if request.HasField("reset_collection") and request.reset_collection:
                segment["collection"] = None
            if request.HasField("metadata"):
                target = cast(Dict[str, Any], segment["metadata"])
                if segment["metadata"] is None:
                    segment["metadata"] = {}
                self._merge_metadata(target, request.metadata)
            if request.HasField("reset_metadata") and request.reset_metadata:
                segment["metadata"] = {}
            return proto.ChromaResponse(status=proto.Status(code=200))

    @overrides(check_signature=False)
    def CreateCollection(
        self, request: CreateCollectionRequest, context: grpc.ServicerContext
    ) -> CreateCollectionResponse:
        collection = from_proto_collection(request.collection)
        if collection["id"].hex in self._collections:
            return CreateCollectionResponse(
                status=proto.Status(
                    code=409, reason=f"Collection {collection['id']} already exists"
                )
            )

        self._collections[collection["id"].hex] = collection
        return CreateCollectionResponse(
            status=proto.Status(code=200),
            collection=to_proto_collection(collection),
        )

    @overrides(check_signature=False)
    def DeleteCollection(
        self, request: DeleteCollectionRequest, context: grpc.ServicerContext
    ) -> proto.ChromaResponse:
        collection_id = request.id
        if collection_id in self._collections:
            del self._collections[collection_id]
            return proto.ChromaResponse(status=proto.Status(code=200))
        else:
            return proto.ChromaResponse(
                status=proto.Status(
                    code=404, reason=f"Collection {collection_id} not found"
                )
            )

    @overrides(check_signature=False)
    def GetCollections(
        self, request: GetCollectionsRequest, context: grpc.ServicerContext
    ) -> GetCollectionsResponse:
        target_id = UUID(hex=request.id) if request.HasField("id") else None
        target_topic = request.topic if request.HasField("topic") else None
        target_name = request.name if request.HasField("name") else None

        found_collections = []
        for collection in self._collections.values():
            if target_id and collection["id"] != target_id:
                continue
            if target_topic and collection["topic"] != target_topic:
                continue
            if target_name and collection["name"] != target_name:
                continue
            found_collections.append(collection)
        return GetCollectionsResponse(
            collections=[
                to_proto_collection(collection) for collection in found_collections
            ]
        )

    @overrides(check_signature=False)
    def UpdateCollection(
        self, request: UpdateCollectionRequest, context: grpc.ServicerContext
    ) -> proto.ChromaResponse:
        id_to_update = UUID(request.id)
        if id_to_update.hex not in self._collections:
            return proto.ChromaResponse(
                status=proto.Status(
                    code=404, reason=f"Collection {id_to_update} not found"
                )
            )
        else:
            collection = self._collections[id_to_update.hex]
            if request.HasField("topic"):
                collection["topic"] = request.topic
            if request.HasField("name"):
                collection["name"] = request.name
            if request.HasField("dimension"):
                collection["dimension"] = request.dimension
            if request.HasField("metadata"):
                # TODO: IN SysDB SQlite we have technical debt where we
                # replace the entire metadata dict with the new one. We should
                # fix that by merging it. For now we just do the same thing here

                update_metadata = from_proto_update_metadata(request.metadata)
                cleaned_metadata = None
                if update_metadata is not None:
                    cleaned_metadata = {}
                    for key, value in update_metadata.items():
                        if value is not None:
                            cleaned_metadata[key] = value

                collection["metadata"] = cleaned_metadata
            elif request.HasField("reset_metadata"):
                if request.reset_metadata:
                    collection["metadata"] = {}

            return proto.ChromaResponse(status=proto.Status(code=200))

    @overrides(check_signature=False)
    def ResetState(
        self, request: Empty, context: grpc.ServicerContext
    ) -> proto.ChromaResponse:
        self.reset_state()
        return proto.ChromaResponse(status=proto.Status(code=200))

    def _merge_metadata(self, target: Metadata, source: proto.UpdateMetadata) -> None:
        target_metadata = cast(Dict[str, Any], target)
        source_metadata = cast(Dict[str, Any], from_proto_update_metadata(source))
        target_metadata.update(source_metadata)
        # If a key has a None value, remove it from the metadata
        for key, value in source_metadata.items():
            if value is None and key in target:
                del target_metadata[key]
