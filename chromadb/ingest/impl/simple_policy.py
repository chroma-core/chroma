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
