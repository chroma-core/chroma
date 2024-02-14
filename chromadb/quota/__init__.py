from abc import abstractmethod
from typing import Optional, Literal

from chromadb import Documents, Embeddings
from chromadb.api import Metadatas
from chromadb.config import (
    Component,
    System,
)
Resource = Literal["METADATA_KEY_LENGTH", "METADATA_VALUE_LENGTH", "DOCUMENT_SIZE", "EMBEDDINGS_DIMENSION"]


class QuotaError(Exception):
    def __init__(self, resource: Resource, quota: int, actual: int):
        super().__init__("Out of quota resource")
        self.quota = quota
        self.actual = actual
        self.resource = resource


class QuotaProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self.system = system

    @abstractmethod
    def get_for_subject(self, resource: Resource, subject: Optional[str] = None, tier: Optional[str] = None) -> Optional[int]:
        pass

class QuotaEnforcer(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self.should_enforce = False
        if system.settings.chroma_quota_provider_impl:
            self._quota_provider = system.require(QuotaProvider)
            self.should_enforce = True
        self.system = system

    def payload_static_check(self, metadatas: Optional[Metadatas] = None, documents: Optional[Documents] = None, embeddings: Optional[Embeddings]= None, collection_id: Optional[str]= None):
        if not self.should_enforce:
            return
        print(embeddings)
        metadata_key_length_quota = self._quota_provider.get_for_subject(resource="METADATA_KEY_LENGTH", subject=collection_id)
        metadata_value_length_quota = self._quota_provider.get_for_subject(resource="METADATA_VALUE_LENGTH", subject=collection_id)
        if metadatas and (metadata_key_length_quota or metadata_key_length_quota):
            for metadata in metadatas:
                for key in metadata:
                    if metadata_key_length_quota and len(key) > metadata_key_length_quota:
                        raise QuotaError(resource="METADATA_KEY_LENGTH", actual=len(key), quota=metadata_key_length_quota)
                    if metadata_value_length_quota and isinstance(metadata[key], str) and len(metadata[key]) > metadata_value_length_quota:
                        raise QuotaError(resource="METADATA_VALUE_LENGTH", actual=len(metadata[key]), quota=metadata_value_length_quota)
        document_size_quota = self._quota_provider.get_for_subject(resource="DOCUMENT_SIZE", subject=collection_id)
        if document_size_quota and documents:
            for document in documents:
                if len(document) > document_size_quota:
                    raise QuotaError(resource="DOCUMENT_SIZE", actual=len(document) , quota=document_size_quota)
        embedding_dimension_quota = self._quota_provider.get_for_subject(resource="EMBEDDINGS_DIMENSION", subject=collection_id)
        if embeddings:
            for embedding in embeddings:
                if len(embedding) > embedding_dimension_quota:
                    raise QuotaError(resource="EMBEDDINGS_DIMENSION", actual=len(embedding), quota=embedding_dimension_quota)