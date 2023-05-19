from abc import ABC, abstractmethod
from typing import Callable, Optional, Sequence
from overrides import EnforceOverrides
from chromadb.types import InsertEmbeddingRecord, EmbeddingRecord, SeqId
from uuid import UUID


class Producer(ABC, EnforceOverrides):
    """Interface for writing embeddings to an ingest stream"""

    @abstractmethod
    def create_topic(self, topic_name: str) -> None:
        pass

    @abstractmethod
    def delete_topic(self, topic_name: str) -> None:
        pass

    @abstractmethod
    def submit_embedding(
        self, topic_name: str, embedding: InsertEmbeddingRecord, sync: bool = False
    ) -> None:
        """Add an embedding record to the given topic."""
        pass

    @abstractmethod
    def submit_embedding_delete(
        self, topic_name: str, id: str, sync: bool = False
    ) -> None:
        """Add an embedding deletion record (soft delete) to the given topic."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Delete all topics and data. For testing only, implementations intended for
        production may throw an exception instead of implementing this method."""
        pass


class Consumer(ABC, EnforceOverrides):
    """Interface for reading embeddings off an ingest stream"""

    @abstractmethod
    def subscribe(
        self,
        topic_name: str,
        consume_fn: Callable[[Sequence[EmbeddingRecord]], bool],
        start: Optional[SeqId] = None,
        end: Optional[SeqId] = None,
        id: Optional[UUID] = None,
    ) -> UUID:
        """Register a function that will be called to recieve embeddings for a given
        topic. The given function may be called any number of times, with any number of
        records, and may be called concurrently.

        The function should return True if and only if the embeddings were successfully
        processed.

        If the function returns False or throws an exception, the function may be called
        again with the same or different records.

        Takes an optional UUID as a unique subscription ID. If no ID is provided, a new
        ID will be generated and returned."""
        pass

    @abstractmethod
    def unsubscribe(self, subscription_id: UUID) -> None:
        """Unregister a subscription. The consume function will no longer be invoked,
        and resources associated with the subscription will be released."""
        pass
