import json
import time
import re
from typing import Dict, List, Optional, Sequence, Callable, Type, cast
import chromadb.config
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from chromadb.types import Topic, InsertEmbeddingRecord, InsertType
import chromadb.ingest
import chromadb.db
import chromadb.segment
from chromadb.api.types import (
    Documents,
    Embedding,
    Embeddings,
    GetResult,
    IDs,
    Include,
    Metadatas,
    QueryResult,
    Where,
    WhereDocument,
)
from chromadb.api.models.Collection import Collection

# Regex for the format "<protocol>://<tenant>/<namespace/<name>"
topic_re = re.compile(r"^([a-zA-Z0-9]+)://([a-zA-Z0-9]+)/([a-zA-Z0-9]+)/([a-zA-Z0-9]+)$")


class DecoupledAPI(API):
    """API that uses the new segment-based architecture in which reads and writes are decoupled."""

    ingest_impl: chromadb.ingest.Producer
    sysdb: chromadb.db.SysDB
    segment_manager: chromadb.segment.SegmentManager

    def __init__(self, settings):
        self.settings = settings
        self.ingest_impl = chromadb.config.get_component(settings, "chroma_ingest_impl")
        self.sysdb = chromadb.config.get_component(settings, "chroma_system_db_impl")
        self.segment_manager = chromadb.config.get_component(settings, "chroma_segment_manager")
        pass

    def heartbeat(self):
        return int(1000 * time.time_ns())

    def _collection(self, topic: Topic):
        """Create a Collection object from a Topic object"""
        match = topic_re.match(topic["name"])

        if match is None:
            raise ValueError(f"Invalid topic name: {topic['name']}")

        _, _, _, name = match.groups()

        return Collection(
            client=self,
            name=name,
            metadata=topic["metadata"],
            embedding_function_name=topic["embedding_function"],
        )

    #
    # COLLECTION METHODS
    #
    def list_collections(self) -> Sequence[Collection]:

        topics = self.sysdb.get_topics()
        collections = []
        for topic in topics:
            collections.append(self._collection(topic))

        return collections

    def _topic(self, name: str) -> str:
        "Given a user-facing collection name, return the fully qualified topic name"
        # Note: this will need to be refined for the case of multitenancy
        return f"persistent://public/default/{name}"

    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict] = {},
        get_or_create: bool = False,
        embedding_function: Optional[Callable] = None,
        embedding_function_name: Optional[str] = None,
    ) -> Collection:

        topics = self.sysdb.get_topics(self._topic(name))

        if len(topics) > 0:
            if get_or_create:
                return self.get_collection(name)
            else:
                raise ValueError(f"Collection {name} already exists")

        topic = Topic(
            name=self._topic(name), metadata=metadata, embedding_function=embedding_function_name
        )

        self.ingest_impl.create_topic(topic)
        if self.ingest_impl != self.sysdb:
            self.sysdb.create_topic(topic)
        if self.segment_manager != self.sysdb:
            self.segment_manager.create_topic_segments(topic)

        return self.get_collection(name)

    def delete_collection(
        self,
        name: str,
    ):
        self.ingest_impl.delete_topic(name)
        self.sysdb.delete_topic(name)
        self.segment_manager.delete_topic_segments(name)

    def get_or_create_collection(self, name: str, metadata: Optional[Dict] = None) -> Collection:
        """Calls create_collection with get_or_create=True

        Args:
            name (str): The name of the collection to create. The name must be unique.
            metadata (Optional[Dict], optional): A dictionary of metadata to associate with the collection. Defaults to None.
        Returns:
            dict: the created collection

        """
        return self.create_collection(name, metadata, get_or_create=True)

    def get_collection(
        self,
        name: str,
        embedding_function: Optional[Callable] = None,
    ) -> Collection:
        if embedding_function is not None:
            raise ValueError("Passing a callable as an embedding function is not supported")
        topics = self.sysdb.get_topics(self._topic(name))
        if len(topics) == 0:
            raise ValueError(f"Collection {name} does not exist")
        return self._collection(topics[0])

    def _modify(
        self,
        current_name: str,
        new_name: Optional[str] = None,
        new_metadata: Optional[Dict] = None,
    ):
        pass

    #
    # ITEM METHODS
    #
    def _add(
        self,
        ids,
        collection_name: str,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ):

        topic = self._topic(collection_name)

        for i, e, m, d in zip(ids, embeddings, metadatas or [], documents or []):

            if d is not None:
                if m is None:
                    m = {"document": d}
                else:
                    m["document"] = d

            metadata = {k: str(v) for k, v in m.items()}

            embedding = InsertEmbeddingRecord(
                id=i, embedding=e, metadata=metadata, insert_type=InsertType.ADD_ONLY
            )
            self.ingest_impl.submit_embedding(topic_name=topic, embedding=embedding)

    def _update(
        self,
        collection_name: str,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ):
        pass

    def _count(self, collection_name: str) -> int:
        pass

    def _get(
        self,
        collection_name: str,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        where_document: Optional[WhereDocument] = {},
        include: Include = ["embeddings", "metadatas", "documents"],
    ) -> GetResult:
        pass

    def _delete(
        self,
        collection_name: str,
        ids: Optional[IDs],
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
    ):
        pass

    def _query(
        self,
        collection_name: str,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Where = {},
        where_document: WhereDocument = {},
        include: Include = ["embeddings", "metadatas", "documents", "distances"],
    ) -> QueryResult:
        pass

    def _peek(self, collection_name: str, n: int = 10) -> GetResult:
        pass

    def reset(self) -> bool:
        if self.settings.enable_system_reset:
            self.segment_manager.reset()
            self.sysdb.reset()
            self.ingest_impl.reset()
            return True
        else:
            raise Exception("System reset is disabled")

    def raw_sql(self, sql: str):
        pass

    def create_index(self, collection_name: Optional[str] = None) -> bool:
        pass
