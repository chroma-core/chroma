from chromadb.api import API
from chromadb.config import System
from chromadb.db.system import SysDB
from chromadb.segment import SegmentManager, MetadataReader, VectorReader
from chromadb.telemetry import Telemetry
from chromadb.ingest import Producer
from chromadb.api.models.Collection import Collection
import chromadb.api.local as old_api
from chromadb import __version__
from chromadb.errors import InvalidDimensionException, InvalidCollectionException

from chromadb.api.types import (
    CollectionMetadata,
    EmbeddingFunction,
    IDs,
    Embeddings,
    Embedding,
    Metadatas,
    Documents,
    Where,
    WhereDocument,
    Include,
    GetResult,
    QueryResult,
    validate_metadata,
    validate_update_metadata,
)

import chromadb.types as t

from typing import Optional, Sequence, Generator, List, cast, Set, Dict
from overrides import override
from uuid import UUID, uuid4
import pandas as pd
import time
import logging

logger = logging.getLogger(__name__)


class SegmentAPI(API):
    """API implementation utilizing the new segment-based internal architecture"""

    _sysdb: SysDB
    _manager: SegmentManager
    _producer: Producer
    _telemetry_client: Telemetry
    _tenant_id: str
    _topic_ns: str
    _collection_cache: Dict[UUID, t.Collection]

    def __init__(self, system: System):
        super().__init__(system)
        self._sysdb = self.require(SysDB)
        self._manager = self.require(SegmentManager)
        self._telemetry_client = self.require(Telemetry)
        self._producer = self.require(Producer)
        self._tenant_id = system.settings.tenant_id
        self._topic_ns = system.settings.topic_namespace
        self._collection_cache = {}

    @override
    def heartbeat(self) -> int:
        return int(time.time_ns())

    # TODO: Actually fix CollectionMetadata type to remove type: ignore flags. This is
    # necessary because changing the value type from `Any` to`` `Union[str, int, float]`
    # causes the system to somehow convert all values to strings.
    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = None,
        get_or_create: bool = False,
    ) -> Collection:
        existing = self._sysdb.get_collections(name=name)

        if metadata is not None:
            validate_metadata(metadata)

        if existing:
            if get_or_create:
                if metadata and existing[0]["metadata"] != metadata:
                    self._modify(id=existing[0]["id"], new_metadata=metadata)
                    existing = self._sysdb.get_collections(id=existing[0]["id"])
                return Collection(
                    client=self,
                    id=existing[0]["id"],
                    name=existing[0]["name"],
                    metadata=existing[0]["metadata"],  # type: ignore
                    embedding_function=embedding_function,
                )
            else:
                raise ValueError(f"Collection {name} already exists.")

        # backwards compatibility in naming requirements (for now)
        old_api.check_index_name(name)

        id = uuid4()
        coll = t.Collection(
            id=id, name=name, metadata=metadata, topic=self._topic(id), dimension=None
        )
        self._producer.create_topic(coll["topic"])
        segments = self._manager.create_segments(coll)
        self._sysdb.create_collection(coll)
        for segment in segments:
            self._sysdb.create_segment(segment)

        return Collection(
            client=self,
            id=id,
            name=name,
            metadata=metadata,
            embedding_function=embedding_function,
        )

    @override
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = None,
    ) -> Collection:
        return self.create_collection(
            name=name,
            metadata=metadata,
            embedding_function=embedding_function,
            get_or_create=True,
        )

    # TODO: Actually fix CollectionMetadata type to remove type: ignore flags. This is
    # necessary because changing the value type from `Any` to`` `Union[str, int, float]`
    # causes the system to somehow convert all values to strings
    @override
    def get_collection(
        self,
        name: str,
        embedding_function: Optional[EmbeddingFunction] = None,
    ) -> Collection:
        existing = self._sysdb.get_collections(name=name)

        if existing:
            return Collection(
                client=self,
                id=existing[0]["id"],
                name=existing[0]["name"],
                metadata=existing[0]["metadata"],  # type: ignore
                embedding_function=embedding_function,
            )
        else:
            raise ValueError(f"Collection {name} does not exist.")

    @override
    def list_collections(self) -> Sequence[Collection]:
        collections = []
        db_collections = self._sysdb.get_collections()
        for db_collection in db_collections:
            collections.append(
                Collection(
                    client=self,
                    id=db_collection["id"],
                    name=db_collection["name"],
                    metadata=db_collection["metadata"],  # type: ignore
                )
            )
        return collections

    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        if new_name:
            # backwards compatibility in naming requirements (for now)
            old_api.check_index_name(new_name)

        if new_metadata:
            validate_update_metadata(new_metadata)

        # TODO eventually we'll want to use OptionalArgument and Unspecified in the
        # signature of `_modify` but not changing the API right now.
        if new_name and new_metadata:
            self._sysdb.update_collection(id, name=new_name, metadata=new_metadata)
        elif new_name:
            self._sysdb.update_collection(id, name=new_name)
        elif new_metadata:
            self._sysdb.update_collection(id, metadata=new_metadata)

    @override
    def delete_collection(self, name: str) -> None:
        existing = self._sysdb.get_collections(name=name)

        if existing:
            self._sysdb.delete_collection(existing[0]["id"])
            for s in self._manager.delete_segments(existing[0]["id"]):
                self._sysdb.delete_segment(s)
            self._producer.delete_topic(existing[0]["topic"])
            if existing and existing[0]["id"] in self._collection_cache:
                del self._collection_cache[existing[0]["id"]]
        else:
            raise ValueError(f"Collection {name} does not exist.")

    @override
    def _add(
        self,
        ids: IDs,
        collection_id: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ) -> bool:
        coll = self._get_collection(collection_id)

        for r in _records(t.Operation.ADD, ids, embeddings, metadatas, documents):
            self._validate_embedding_record(coll, r)
            self._producer.submit_embedding(coll["topic"], r)

        return True

    @override
    def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ) -> bool:
        coll = self._get_collection(collection_id)

        for r in _records(t.Operation.UPDATE, ids, embeddings, metadatas, documents):
            self._validate_embedding_record(coll, r)
            self._producer.submit_embedding(coll["topic"], r)

        return True

    @override
    def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ) -> bool:
        coll = self._get_collection(collection_id)
        for r in _records(t.Operation.UPSERT, ids, embeddings, metadatas, documents):
            self._validate_embedding_record(coll, r)
            self._producer.submit_embedding(coll["topic"], r)

        return True

    @override
    def _get(
        self,
        collection_id: UUID,
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
        metadata_segment = self._manager.get_segment(collection_id, MetadataReader)

        if sort is not None:
            raise NotImplementedError("Sorting is not yet supported")

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        records = metadata_segment.get_metadata(
            where=where,
            where_document=where_document,
            ids=ids,
            limit=limit,
            offset=offset,
        )

        vectors = None
        if "embeddings" in include:
            vector_ids = [r["id"] for r in records]
            vector_segment = self._manager.get_segment(collection_id, VectorReader)
            vectors = vector_segment.get_vectors(ids=vector_ids)

        # TODO: Fix type so we don't need to ignore
        # It is possible to have a set of records, some with metadata and some without
        # Same with documents

        metadatas = [r["metadata"] for r in records]

        if "documents" in include:
            documents = [_doc(m) for m in metadatas]

        return GetResult(
            ids=[r["id"] for r in records],
            embeddings=[r["embedding"] for r in vectors] if vectors else None,
            metadatas=_clean_metadatas(metadatas) if "metadatas" in include else None,  # type: ignore
            documents=documents if "documents" in include else None,  # type: ignore
        )

    @override
    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
    ) -> IDs:
        coll = self._get_collection(collection_id)

        # TODO: Do we want to warn the user that unrestricted _delete() is 99% of the
        # time a bad idea?
        if (where or where_document) or not ids:
            metadata_segment = self._manager.get_segment(collection_id, MetadataReader)
            records = metadata_segment.get_metadata(
                where=where, where_document=where_document, ids=ids
            )
            ids_to_delete = [r["id"] for r in records]
        else:
            ids_to_delete = ids

        for r in _records(t.Operation.DELETE, ids_to_delete):
            self._validate_embedding_record(coll, r)
            self._producer.submit_embedding(coll["topic"], r)

        return ids_to_delete

    @override
    def _count(self, collection_id: UUID) -> int:
        metadata_segment = self._manager.get_segment(collection_id, MetadataReader)
        return metadata_segment.count()

    @override
    def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Where = {},
        where_document: WhereDocument = {},
        include: Include = ["documents", "metadatas", "distances"],
    ) -> QueryResult:
        allowed_ids = None

        coll = self._get_collection(collection_id)
        for embedding in query_embeddings:
            self._validate_dimension(coll, len(embedding), update=False)

        metadata_reader = self._manager.get_segment(collection_id, MetadataReader)

        if where or where_document:
            records = metadata_reader.get_metadata(
                where=where, where_document=where_document
            )
            allowed_ids = [r["id"] for r in records]

        query = t.VectorQuery(
            vectors=query_embeddings,
            k=n_results,
            allowed_ids=allowed_ids,
            include_embeddings="embeddings" in include,
            options=None,
        )

        vector_reader = self._manager.get_segment(collection_id, VectorReader)
        results = vector_reader.query_vectors(query)

        ids: List[List[str]] = []
        distances: List[List[float]] = []
        embeddings: List[List[Embedding]] = []
        documents: List[List[str]] = []
        metadatas: List[List[t.Metadata]] = []

        for result in results:
            ids.append([r["id"] for r in result])
            if "distances" in include:
                distances.append([r["distance"] for r in result])
            if "embeddings" in include:
                embeddings.append([cast(Embedding, r["embedding"]) for r in result])

        if "documents" in include or "metadatas" in include:
            all_ids: Set[str] = set()
            for id_list in ids:
                all_ids.update(id_list)
            records = metadata_reader.get_metadata(ids=list(all_ids))
            metadata_by_id = {r["id"]: r["metadata"] for r in records}
            for id_list in ids:
                metadata_list = [metadata_by_id[id] for id in id_list]
                if "metadatas" in include:
                    metadatas.append(_clean_metadatas(metadata_list))  # type: ignore
                if "documents" in include:
                    doc_list = [_doc(m) for m in metadata_list]
                    documents.append(doc_list)  # type: ignore

        return QueryResult(
            ids=ids,
            distances=distances if distances else None,
            metadatas=metadatas if metadatas else None,
            embeddings=embeddings if embeddings else None,
            documents=documents if documents else None,
        )

    @override
    def _peek(self, collection_id: UUID, n: int = 10) -> GetResult:
        return self._get(collection_id, limit=n)

    @override
    def get_version(self) -> str:
        return __version__

    @override
    def reset_state(self) -> None:
        self._collection_cache = {}

    @override
    def reset(self) -> bool:
        self._system.reset_state()
        return True

    @override
    def raw_sql(self, sql: str) -> pd.DataFrame:
        raise NotImplementedError()

    @override
    def create_index(self, collection_name: str) -> bool:
        logger.warning(
            "Calling create_index is unnecessary, data is now automatically indexed"
        )
        return True

    @override
    def persist(self) -> bool:
        logger.warning(
            "Calling persist is unnecessary, data is now automatically indexed."
        )
        return True

    def _topic(self, collection_id: UUID) -> str:
        return f"persistent://{self._tenant_id}/{self._topic_ns}/{collection_id}"

    # TODO: This could potentially cause race conditions in a distributed version of the
    # system, since the cache is only local.
    def _validate_embedding_record(
        self, collection: t.Collection, record: t.SubmitEmbeddingRecord
    ) -> None:
        """Validate the dimension of an embedding record before submitting it to the system."""
        if record["embedding"]:
            self._validate_dimension(collection, len(record["embedding"]), update=True)

    def _validate_dimension(
        self, collection: t.Collection, dim: int, update: bool
    ) -> None:
        """Validate that a collection supports records of the given dimension. If update
        is true, update the collection if the collection doesn't already have a
        dimension."""

        if collection["dimension"] is None:
            if update:
                id = collection["id"]
                self._sysdb.update_collection(id=id, dimension=dim)
                self._collection_cache[id]["dimension"] = dim
        elif collection["dimension"] != dim:
            raise InvalidDimensionException(
                f"Embedding dimension {dim} does not match collection dimensionality {collection['dimension']}"
            )
        else:
            return  # all is well

    def _get_collection(self, collection_id: UUID) -> t.Collection:
        """Read-through cache for collection data"""
        if collection_id not in self._collection_cache:
            collections = self._sysdb.get_collections(id=collection_id)
            if not collections:
                raise InvalidCollectionException(
                    f"Collection {collection_id} does not exist."
                )
            self._collection_cache[collection_id] = collections[0]
        return self._collection_cache[collection_id]


def _records(
    operation: t.Operation,
    ids: IDs,
    embeddings: Optional[Embeddings] = None,
    metadatas: Optional[Metadatas] = None,
    documents: Optional[Documents] = None,
) -> Generator[t.SubmitEmbeddingRecord, None, None]:
    """Convert parallel lists of embeddings, metadatas and documents to a sequence of
    SubmitEmbeddingRecords"""

    # Presumes that callers were invoked via  Collection model, which means
    # that we know that the embeddings, metadatas and documents have already been
    # normalized and are guaranteed to be consistently named lists.

    # TODO: Fix API types to make it explicit that they've already been normalized

    for i, id in enumerate(ids):
        metadata = None
        if metadatas:
            metadata = metadatas[i]

        if documents:
            document = documents[i]
            if metadata:
                metadata = {**metadata, "chroma:document": document}
            else:
                metadata = {"chroma:document": document}

        record = t.SubmitEmbeddingRecord(
            id=id,
            embedding=embeddings[i] if embeddings else None,
            encoding=t.ScalarEncoding.FLOAT32,  # Hardcode for now
            metadata=metadata,
            operation=operation,
        )
        yield record


def _doc(metadata: Optional[t.Metadata]) -> Optional[str]:
    """Retrieve the document (if any) from a Metadata map"""

    if metadata and "chroma:document" in metadata:
        return str(metadata["chroma:document"])
    return None


def _clean_metadatas(
    metadata: List[Optional[t.Metadata]],
) -> List[Optional[t.Metadata]]:
    """Remove any chroma-specific metadata keys that the client shouldn't see from a
    list of metadata maps."""
    return [_clean_metadata(m) for m in metadata]


def _clean_metadata(metadata: Optional[t.Metadata]) -> Optional[t.Metadata]:
    """Remove any chroma-specific metadata keys that the client shouldn't see from a
    metadata map."""
    if not metadata:
        return None
    result = {}
    for k, v in metadata.items():
        if not k.startswith("chroma:"):
            result[k] = v
    return result
