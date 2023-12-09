from typing import Sequence
from uuid import UUID
from overrides import overrides
from chromadb.config import System
from chromadb.ingest import CollectionAssignmentPolicy
from chromadb.ingest.impl.utils import create_topic_name


class SimpleAssignmentPolicy(CollectionAssignmentPolicy):
    """Simple assignment policy that assigns a 1 collection to 1 topic based on the
    id of the collection."""

    _tenant_id: str
    _topic_ns: str

    def __init__(self, system: System):
        self._tenant_id = system.settings.tenant_id
        self._topic_ns = system.settings.topic_namespace
        super().__init__(system)

    def _topic(self, collection_id: UUID) -> str:
        return create_topic_name(self._tenant_id, self._topic_ns, str(collection_id))

    @overrides
    def assign_collection(self, collection_id: UUID) -> str:
        return self._topic(collection_id)

    @overrides
    def get_topics(self) -> Sequence[str]:
        raise NotImplementedError(
            "SimpleAssignmentPolicy does not support get_topics, each collection has its own topic"
        )


class RendezvousHashingAssignmentPolicy(CollectionAssignmentPolicy):
    """The rendezvous hashing assignment policy assigns a collection to a topic based on the
    rendezvous hashing algorithm. This is not actually used in the python sysdb. It is only used in the
    go sysdb. However, it is useful here in order to provide a way to get the topic list used for the whole system.
    """

    _tenant_id: str
    _topic_ns: str

    def __init__(self, system: System):
        self._tenant_id = system.settings.tenant_id
        self._topic_ns = system.settings.topic_namespace
        super().__init__(system)

    @overrides
    def assign_collection(self, collection_id: UUID) -> str:
        raise NotImplementedError(
            "RendezvousHashingAssignmentPolicy is not implemented"
        )

    @overrides
    def get_topics(self) -> Sequence[str]:
        # Mirrors go/coordinator/internal/coordinator/assignment_policy.go
        return [
            f"persistent://{self._tenant_id}/{self._topic_ns}/chroma_log_{i}"
            for i in range(16)
        ]
