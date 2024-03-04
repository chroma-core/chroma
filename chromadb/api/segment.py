from chromadb.api import ServerAPI
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.db.system import SysDB
from chromadb.quota import QuotaEnforcer, Resource
from chromadb.rate_limiting import rate_limit
from chromadb.segment import SegmentManager, MetadataReader, VectorReader
from chromadb.telemetry.opentelemetry import (
    add_attributes_to_current_span,
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.telemetry.product import ProductTelemetryClient
from chromadb.ingest import Producer
from chromadb.api.models.Collection import Collection
from chromadb import __version__
from chromadb.errors import InvalidDimensionException, InvalidCollectionException
import chromadb.utils.embedding_functions as ef

from chromadb.api.types import (
    URI,
    CollectionMetadata,
    Embeddable,
    Document,
    EmbeddingFunction,
    DataLoader,
    IDs,
    Embeddings,
    Embedding,
    Loadable,
    Metadatas,
    Documents,
    URIs,
    Where,
    WhereDocument,
    Include,
    GetResult,
    QueryResult,
    validate_metadata,
    validate_update_metadata,
    validate_where,
    validate_where_document,
    validate_batch,
)
from chromadb.telemetry.product.events import (
    CollectionAddEvent,
    CollectionDeleteEvent,
    CollectionGetEvent,
    CollectionUpdateEvent,
    CollectionQueryEvent,
    ClientCreateCollectionEvent,
)

import chromadb.types as t

from typing import Any, Optional, Sequence, Generator, List, cast, Set, Dict
from overrides import override
from uuid import UUID, uuid4
import time
import logging
import re

logger = logging.getLogger(__name__)


# mimics s3 bucket requirements for naming
def check_index_name(index_name: str) -> None:
    msg = (
        "Expected collection name that "
        "(1) contains 3-63 characters, "
        "(2) starts and ends with an alphanumeric character, "
        "(3) otherwise contains only alphanumeric characters, underscores or hyphens (-), "
        "(4) contains no two consecutive periods (..) and "
        "(5) is not a valid IPv4 address, "
        f"got {index_name}"
    )
    if len(index_name) < 3 or len(index_name) > 63:
        raise ValueError(msg)
    if not re.match("^[a-zA-Z0-9][a-zA-Z0-9._-]*[a-zA-Z0-9]$", index_name):
        raise ValueError(msg)
    if ".." in index_name:
        raise ValueError(msg)
    if re.match("^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$", index_name):
        raise ValueError(msg)


class SegmentAPI(ServerAPI):
    """API implementation utilizing the new segment-based internal architecture"""

    _settings: Settings
    _sysdb: SysDB
    _manager: SegmentManager
    _producer: Producer
    _product_telemetry_client: ProductTelemetryClient
    _opentelemetry_client: OpenTelemetryClient
    _tenant_id: str
    _topic_ns: str
    _collection_cache: Dict[UUID, t.Collection]

    def __init__(self, system: System):
        super().__init__(system)
        self._settings = system.settings
        self._sysdb = self.require(SysDB)
        self._manager = self.require(SegmentManager)
        self._quota = self.require(QuotaEnforcer)
        self._product_telemetry_client = self.require(ProductTelemetryClient)
        self._opentelemetry_client = self.require(OpenTelemetryClient)
        self._producer = self.require(Producer)
        self._collection_cache = {}

    @override
    def heartbeat(self) -> int:
        return int(time.time_ns())

    @override
    def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        if len(name) < 3:
            raise ValueError("Database name must be at least 3 characters long")

        self._sysdb.create_database(
            id=uuid4(),
            name=name,
            tenant=tenant,
        )

    @override
    def get_database(self, name: str, tenant: str = DEFAULT_TENANT) -> t.Database:
        return self._sysdb.get_database(name=name, tenant=tenant)

    @override
    def create_tenant(self, name: str) -> None:
        if len(name) < 3:
            raise ValueError("Tenant name must be at least 3 characters long")

        self._sysdb.create_tenant(
            name=name,
        )

    @override
    def get_tenant(self, name: str) -> t.Tenant:
        return self._sysdb.get_tenant(name=name)

    # TODO: Actually fix CollectionMetadata type to remove type: ignore flags. This is
    # necessary because changing the value type from `Any` to`` `Union[str, int, float]`
    # causes the system to somehow convert all values to strings.
    @trace_method("SegmentAPI.create_collection", OpenTelemetryGranularity.OPERATION)
    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Any]
        ] = ef.DefaultEmbeddingFunction(),
        data_loader: Optional[DataLoader[Loadable]] = None,
        get_or_create: bool = False,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Collection:
        if metadata is not None:
            validate_metadata(metadata)

        # TODO: remove backwards compatibility in naming requirements
        check_index_name(name)

        id = uuid4()

        coll, created = self._sysdb.create_collection(
            id=id,
            name=name,
            metadata=metadata,
            dimension=None,
            get_or_create=get_or_create,
            tenant=tenant,
            database=database,
        )

        if created:
            segments = self._manager.create_segments(coll)
            for segment in segments:
                self._sysdb.create_segment(segment)

        # TODO: This event doesn't capture the get_or_create case appropriately
        self._product_telemetry_client.capture(
            ClientCreateCollectionEvent(
                collection_uuid=str(id),
                embedding_function=embedding_function.__class__.__name__,
            )
        )
        add_attributes_to_current_span({"collection_uuid": str(id)})

        return Collection(
            client=self,
            id=coll["id"],
            name=name,
            metadata=coll["metadata"],  # type: ignore
            embedding_function=embedding_function,
            data_loader=data_loader,
            tenant=tenant,
            database=database,
        )

    @trace_method(
        "SegmentAPI.get_or_create_collection", OpenTelemetryGranularity.OPERATION
    )
    @override
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Collection:
        return self.create_collection(  # type: ignore
            name=name,
            metadata=metadata,
            embedding_function=embedding_function,
            data_loader=data_loader,
            get_or_create=True,
            tenant=tenant,
            database=database,
        )

    # TODO: Actually fix CollectionMetadata type to remove type: ignore flags. This is
    # necessary because changing the value type from `Any` to`` `Union[str, int, float]`
    # causes the system to somehow convert all values to strings
    @trace_method("SegmentAPI.get_collection", OpenTelemetryGranularity.OPERATION)
    @override
    def get_collection(
        self,
        name: Optional[str] = None,
        id: Optional[UUID] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Collection:
        if id is None and name is None or (id is not None and name is not None):
            raise ValueError("Name or id must be specified, but not both")
        existing = self._sysdb.get_collections(
            id=id, name=name, tenant=tenant, database=database
        )

        if existing:
            return Collection(
                client=self,
                id=existing[0]["id"],
                name=existing[0]["name"],
                metadata=existing[0]["metadata"],  # type: ignore
                embedding_function=embedding_function,
                data_loader=data_loader,
                tenant=existing[0]["tenant"],
                database=existing[0]["database"],
            )
        else:
            raise ValueError(f"Collection {name} does not exist.")

    @trace_method("SegmentAPI.list_collection", OpenTelemetryGranularity.OPERATION)
    @override
    def list_collections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[Collection]:
        collections = []
        db_collections = self._sysdb.get_collections(
            limit=limit, offset=offset, tenant=tenant, database=database
        )
        for db_collection in db_collections:
            collections.append(
                Collection(
                    client=self,
                    id=db_collection["id"],
                    name=db_collection["name"],
                    metadata=db_collection["metadata"],  # type: ignore
                    tenant=db_collection["tenant"],
                    database=db_collection["database"],
                )
            )
        return collections

    @trace_method("SegmentAPI.count_collections", OpenTelemetryGranularity.OPERATION)
    @override
    def count_collections(
        self,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> int:
        collection_count = len(
            self._sysdb.get_collections(tenant=tenant, database=database)
        )

        return collection_count

    @trace_method("SegmentAPI._modify", OpenTelemetryGranularity.OPERATION)
    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        if new_name:
            # backwards compatibility in naming requirements (for now)
            check_index_name(new_name)

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

    @trace_method("SegmentAPI.delete_collection", OpenTelemetryGranularity.OPERATION)
    @override
    def delete_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        existing = self._sysdb.get_collections(
            name=name, tenant=tenant, database=database
        )

        if existing:
            self._sysdb.delete_collection(
                existing[0]["id"], tenant=tenant, database=database
            )
            for s in self._manager.delete_segments(existing[0]["id"]):
                self._sysdb.delete_segment(s)
            if existing and existing[0]["id"] in self._collection_cache:
                del self._collection_cache[existing[0]["id"]]
        else:
            raise ValueError(f"Collection {name} does not exist.")

    @trace_method("SegmentAPI._add", OpenTelemetryGranularity.OPERATION)
    @rate_limit(subject="collection_id", resource=Resource.ADD_PER_MINUTE)
    @override
    def _add(
        self,
        ids: IDs,
        collection_id: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> bool:
        self._quota.static_check(metadatas, documents, embeddings, collection_id)
        coll = self._get_collection(collection_id)
        self._manager.hint_use_collection(collection_id, t.Operation.ADD)
        validate_batch(
            (ids, embeddings, metadatas, documents, uris),
            {"max_batch_size": self.max_batch_size},
        )
        records_to_submit = []
        for r in _records(
            t.Operation.ADD,
            ids=ids,
            collection_id=collection_id,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
        ):
            self._validate_embedding_record(coll, r)
            records_to_submit.append(r)
        self._producer.submit_embeddings(coll["topic"], records_to_submit)

        self._product_telemetry_client.capture(
            CollectionAddEvent(
                collection_uuid=str(collection_id),
                add_amount=len(ids),
                with_metadata=len(ids) if metadatas is not None else 0,
                with_documents=len(ids) if documents is not None else 0,
                with_uris=len(ids) if uris is not None else 0,
            )
        )
        return True

    @trace_method("SegmentAPI._update", OpenTelemetryGranularity.OPERATION)
    @override
    def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> bool:
        self._quota.static_check(metadatas, documents, embeddings, collection_id)
        coll = self._get_collection(collection_id)
        self._manager.hint_use_collection(collection_id, t.Operation.UPDATE)
        validate_batch(
            (ids, embeddings, metadatas, documents, uris),
            {"max_batch_size": self.max_batch_size},
        )
        records_to_submit = []
        for r in _records(
            t.Operation.UPDATE,
            ids=ids,
            collection_id=collection_id,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
        ):
            self._validate_embedding_record(coll, r)
            records_to_submit.append(r)
        self._producer.submit_embeddings(coll["topic"], records_to_submit)

        self._product_telemetry_client.capture(
            CollectionUpdateEvent(
                collection_uuid=str(collection_id),
                update_amount=len(ids),
                with_embeddings=len(embeddings) if embeddings else 0,
                with_metadata=len(metadatas) if metadatas else 0,
                with_documents=len(documents) if documents else 0,
                with_uris=len(uris) if uris else 0,
            )
        )

        return True

    @trace_method("SegmentAPI._upsert", OpenTelemetryGranularity.OPERATION)
    @override
    def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> bool:
        self._quota.static_check(metadatas, documents, embeddings, collection_id)
        coll = self._get_collection(collection_id)
        self._manager.hint_use_collection(collection_id, t.Operation.UPSERT)
        validate_batch(
            (ids, embeddings, metadatas, documents, uris),
            {"max_batch_size": self.max_batch_size},
        )
        records_to_submit = []
        for r in _records(
            t.Operation.UPSERT,
            ids=ids,
            collection_id=collection_id,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
        ):
            self._validate_embedding_record(coll, r)
            records_to_submit.append(r)
        self._producer.submit_embeddings(coll["topic"], records_to_submit)

        return True

    @trace_method("SegmentAPI._get", OpenTelemetryGranularity.OPERATION)
    @rate_limit(subject="collection_id", resource=Resource.GET_PER_MINUTE)
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
        add_attributes_to_current_span(
            {
                "collection_id": str(collection_id),
                "ids_count": len(ids) if ids else 0,
            }
        )

        where = validate_where(where) if where is not None and len(where) > 0 else None
        where_document = (
            validate_where_document(where_document)
            if where_document is not None and len(where_document) > 0
            else None
        )

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

        if len(records) == 0:
            # Nothing to return if there are no records
            return GetResult(
                ids=[],
                embeddings=[] if "embeddings" in include else None,
                metadatas=[] if "metadatas" in include else None,
                documents=[] if "documents" in include else None,
                uris=[] if "uris" in include else None,
                data=[] if "data" in include else None,
            )

        vectors: Sequence[t.VectorEmbeddingRecord] = []
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

        if "uris" in include:
            uris = [_uri(m) for m in metadatas]

        ids_amount = len(ids) if ids else 0
        self._product_telemetry_client.capture(
            CollectionGetEvent(
                collection_uuid=str(collection_id),
                ids_count=ids_amount,
                limit=limit if limit else 0,
                include_metadata=ids_amount if "metadatas" in include else 0,
                include_documents=ids_amount if "documents" in include else 0,
                include_uris=ids_amount if "uris" in include else 0,
            )
        )

        return GetResult(
            ids=[r["id"] for r in records],
            embeddings=[r["embedding"] for r in vectors]
            if "embeddings" in include
            else None,
            metadatas=_clean_metadatas(metadatas)
            if "metadatas" in include
            else None,  # type: ignore
            documents=documents if "documents" in include else None,  # type: ignore
            uris=uris if "uris" in include else None,  # type: ignore
            data=None,
        )

    @trace_method("SegmentAPI._delete", OpenTelemetryGranularity.OPERATION)
    @override
    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
    ) -> IDs:
        add_attributes_to_current_span(
            {
                "collection_id": str(collection_id),
                "ids_count": len(ids) if ids else 0,
            }
        )

        where = validate_where(where) if where is not None and len(where) > 0 else None
        where_document = (
            validate_where_document(where_document)
            if where_document is not None and len(where_document) > 0
            else None
        )

        # You must have at least one of non-empty ids, where, or where_document.
        if (
            (ids is None or (ids is not None and len(ids) == 0))
            and (where is None or (where is not None and len(where) == 0))
            and (
                where_document is None
                or (where_document is not None and len(where_document) == 0)
            )
        ):
            raise ValueError(
                """
                You must provide either ids, where, or where_document to delete. If
                you want to delete all data in a collection you can delete the
                collection itself using the delete_collection method. Or alternatively,
                you can get() all the relevant ids and then delete them.
                """
            )

        coll = self._get_collection(collection_id)
        self._manager.hint_use_collection(collection_id, t.Operation.DELETE)

        if (where or where_document) or not ids:
            metadata_segment = self._manager.get_segment(collection_id, MetadataReader)
            records = metadata_segment.get_metadata(
                where=where, where_document=where_document, ids=ids
            )
            ids_to_delete = [r["id"] for r in records]
        else:
            ids_to_delete = ids

        if len(ids_to_delete) == 0:
            return []

        records_to_submit = []
        for r in _records(
            operation=t.Operation.DELETE, ids=ids_to_delete, collection_id=collection_id
        ):
            self._validate_embedding_record(coll, r)
            records_to_submit.append(r)
        self._producer.submit_embeddings(coll["topic"], records_to_submit)

        self._product_telemetry_client.capture(
            CollectionDeleteEvent(
                collection_uuid=str(collection_id), delete_amount=len(ids_to_delete)
            )
        )
        return ids_to_delete

    @trace_method("SegmentAPI._count", OpenTelemetryGranularity.OPERATION)
    @override
    def _count(self, collection_id: UUID) -> int:
        add_attributes_to_current_span({"collection_id": str(collection_id)})
        metadata_segment = self._manager.get_segment(collection_id, MetadataReader)
        return metadata_segment.count()

    @trace_method("SegmentAPI._query", OpenTelemetryGranularity.OPERATION)
    @rate_limit(subject="collection_id", resource=Resource.QUERY_PER_MINUTE)
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
        add_attributes_to_current_span(
            {
                "collection_id": str(collection_id),
                "n_results": n_results,
                "where": str(where),
            }
        )
        where = validate_where(where) if where is not None and len(where) > 0 else where
        where_document = (
            validate_where_document(where_document)
            if where_document is not None and len(where_document) > 0
            else where_document
        )

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
        documents: List[List[Document]] = []
        uris: List[List[URI]] = []
        metadatas: List[List[t.Metadata]] = []

        for result in results:
            ids.append([r["id"] for r in result])
            if "distances" in include:
                distances.append([r["distance"] for r in result])
            if "embeddings" in include:
                embeddings.append([cast(Embedding, r["embedding"]) for r in result])

        if "documents" in include or "metadatas" in include or "uris" in include:
            all_ids: Set[str] = set()
            for id_list in ids:
                all_ids.update(id_list)
            records = metadata_reader.get_metadata(ids=list(all_ids))
            metadata_by_id = {r["id"]: r["metadata"] for r in records}
            for id_list in ids:
                # In the segment based architecture, it is possible for one segment
                # to have a record that another segment does not have. This results in
                # data inconsistency. For the case of the local segments and the
                # local segment manager, there is a case where a thread writes
                # a record to the vector segment but not the metadata segment.
                # Then a query'ing thread reads from the vector segment and
                # queries the metadata segment. The metadata segment does not have
                # the record. In this case we choose to return potentially
                # incorrect data in the form of None.
                metadata_list = [metadata_by_id.get(id, None) for id in id_list]
                if "metadatas" in include:
                    metadatas.append(_clean_metadatas(metadata_list))  # type: ignore
                if "documents" in include:
                    doc_list = [_doc(m) for m in metadata_list]
                    documents.append(doc_list)  # type: ignore
                if "uris" in include:
                    uri_list = [_uri(m) for m in metadata_list]
                    uris.append(uri_list)  # type: ignore

        query_amount = len(query_embeddings)
        self._product_telemetry_client.capture(
            CollectionQueryEvent(
                collection_uuid=str(collection_id),
                query_amount=query_amount,
                n_results=n_results,
                with_metadata_filter=query_amount if where is not None else 0,
                with_document_filter=query_amount if where_document is not None else 0,
                include_metadatas=query_amount if "metadatas" in include else 0,
                include_documents=query_amount if "documents" in include else 0,
                include_uris=query_amount if "uris" in include else 0,
                include_distances=query_amount if "distances" in include else 0,
            )
        )

        return QueryResult(
            ids=ids,
            distances=distances if distances else None,
            metadatas=metadatas if metadatas else None,
            embeddings=embeddings if embeddings else None,
            documents=documents if documents else None,
            uris=uris if uris else None,
            data=None,
        )

    @trace_method("SegmentAPI._peek", OpenTelemetryGranularity.OPERATION)
    @override
    def _peek(self, collection_id: UUID, n: int = 10) -> GetResult:
        add_attributes_to_current_span({"collection_id": str(collection_id)})
        return self._get(collection_id, limit=n)  # type: ignore

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
    def get_settings(self) -> Settings:
        return self._settings

    @property
    @override
    def max_batch_size(self) -> int:
        return self._producer.max_batch_size

    # TODO: This could potentially cause race conditions in a distributed version of the
    # system, since the cache is only local.
    # TODO: promote collection -> topic to a base class method so that it can be
    # used for channel assignment in the distributed version of the system.
    @trace_method("SegmentAPI._validate_embedding_record", OpenTelemetryGranularity.ALL)
    def _validate_embedding_record(
        self, collection: t.Collection, record: t.SubmitEmbeddingRecord
    ) -> None:
        """Validate the dimension of an embedding record before submitting it to the system."""
        add_attributes_to_current_span({"collection_id": str(collection["id"])})
        if record["embedding"]:
            self._validate_dimension(collection, len(record["embedding"]), update=True)

    @trace_method("SegmentAPI._validate_dimension", OpenTelemetryGranularity.ALL)
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

    @trace_method("SegmentAPI._get_collection", OpenTelemetryGranularity.ALL)
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
    collection_id: UUID,
    embeddings: Optional[Embeddings] = None,
    metadatas: Optional[Metadatas] = None,
    documents: Optional[Documents] = None,
    uris: Optional[URIs] = None,
) -> Generator[t.SubmitEmbeddingRecord, None, None]:
    """Convert parallel lists of embeddings, metadatas and documents to a sequence of
    SubmitEmbeddingRecords"""

    # Presumes that callers were invoked via  Collection model, which means
    # that we know that the embeddings, metadatas and documents have already been
    # normalized and are guaranteed to be consistently named lists.

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

        if uris:
            uri = uris[i]
            if metadata:
                metadata = {**metadata, "chroma:uri": uri}
            else:
                metadata = {"chroma:uri": uri}

        record = t.SubmitEmbeddingRecord(
            id=id,
            embedding=embeddings[i] if embeddings else None,
            encoding=t.ScalarEncoding.FLOAT32,  # Hardcode for now
            metadata=metadata,
            operation=operation,
            collection_id=collection_id,
        )
        yield record


def _doc(metadata: Optional[t.Metadata]) -> Optional[str]:
    """Retrieve the document (if any) from a Metadata map"""

    if metadata and "chroma:document" in metadata:
        return str(metadata["chroma:document"])
    return None


def _uri(metadata: Optional[t.Metadata]) -> Optional[str]:
    """Retrieve the uri (if any) from a Metadata map"""

    if metadata and "chroma:uri" in metadata:
        return str(metadata["chroma:uri"])
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
    if len(result) == 0:
        return None
    return result
