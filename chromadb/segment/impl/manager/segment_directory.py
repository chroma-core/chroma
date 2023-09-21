from abc import ABC, abstractmethod
from typing import Callable

from overrides import EnforceOverrides, override
from chromadb.config import Component
from chromadb.segment import SegmentDirectory

from chromadb.types import Segment


class DockerComposeSegmentDirectory(SegmentDirectory, EnforceOverrides):
    """A segment directory that uses docker-compose to manage segment endpoints"""

    @override
    def get_segment_endpoint(self, segment: Segment) -> str:
        # This is just a stub for now, as we don't have a real coordinator to assign and manage this
        return "segment-server:50051"

    @override
    def register_updated_segment_callback(
        self, callback: Callable[[Segment], None]
    ) -> None:
        # Updates are not supported for docker-compose yet, as there is only a single, static
        # indexing node
        pass


class KubernetesSegmentDirectory(SegmentDirectory, EnforceOverrides):
    @override
    def get_segment_endpoint(self, segment: Segment) -> str:
        return "segment-server.chroma:50051"

    @override
    def register_updated_segment_callback(
        self, callback: Callable[[Segment], None]
    ) -> None:
        # Updates are not supported for docker-compose yet, as there is only a single, static
        # indexing node
        pass
