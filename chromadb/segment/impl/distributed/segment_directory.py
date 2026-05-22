"""Compatibility stubs for the removed Python distributed segment directory stack.

The distributed segment directory/memberlist implementation was removed in favor of
Rust-backed orchestration. Importing these classes now raises a clear runtime error so callers have
an explicit failure mode when still referencing this path.
"""

from typing import Callable, List

from chromadb.config import System
from chromadb.segment.distributed import Member, Memberlist, MemberlistProvider, SegmentDirectory
from chromadb.types import Segment

_ERROR_MSG = (
    "Python-side distributed segment directory is no longer supported. "
    "Use the Rust backend services for distributed routing instead."
)


def _raise_removed() -> None:
    raise RuntimeError(_ERROR_MSG)


class MockMemberlistProvider(MemberlistProvider):
    """Deprecated compatibility stub for a removed mock memberlist provider."""

    def __init__(self, system: System):
        super().__init__(system)
        _raise_removed()

    def get_memberlist(self) -> Memberlist:
        _raise_removed()
        return []

    def set_memberlist_name(self, memberlist: str) -> None:
        _raise_removed()

    def register_updated_memberlist_callback(
        self, callback: Callable[[Memberlist], None]
    ) -> None:
        _raise_removed()

    def unregister_updated_memberlist_callback(
        self, callback: Callable[[Memberlist], None]
    ) -> bool:
        _raise_removed()
        return False


class CustomResourceMemberlistProvider(MemberlistProvider):
    """Deprecated compatibility stub for a removed CR-backed memberlist provider."""

    def __init__(self, system: System):
        super().__init__(system)
        _raise_removed()

    def get_memberlist(self) -> Memberlist:
        _raise_removed()
        return []

    def set_memberlist_name(self, memberlist: str) -> None:
        _raise_removed()

    def register_updated_memberlist_callback(
        self, callback: Callable[[Memberlist], None]
    ) -> None:
        _raise_removed()

    def unregister_updated_memberlist_callback(
        self, callback: Callable[[Memberlist], None]
    ) -> bool:
        _raise_removed()
        return False


class RendezvousHashSegmentDirectory(SegmentDirectory):
    """Deprecated compatibility stub for a removed rendezvous hash directory."""

    def __init__(self, system: System):
        super().__init__(system)
        _raise_removed()

    def get_segment_endpoints(self, segment: Segment, n: int) -> List[str]:
        _raise_removed()
        return []

    def register_updated_segment_callback(
        self, callback: Callable[[Segment], None]
    ) -> None:
        _raise_removed()


# Retain constants that may be referenced by older code paths.
WATCH_TIMEOUT_SECONDS = 60
KUBERNETES_NAMESPACE = "chroma"
KUBERNETES_GROUP = "chroma.cluster"
HEADLESS_SERVICE = "svc.cluster.local"

__all__ = [
    "MockMemberlistProvider",
    "CustomResourceMemberlistProvider",
    "RendezvousHashSegmentDirectory",
    "WATCH_TIMEOUT_SECONDS",
    "KUBERNETES_NAMESPACE",
    "KUBERNETES_GROUP",
    "HEADLESS_SERVICE",
]
