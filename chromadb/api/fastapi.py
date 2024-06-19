import orjson as json
import logging
from typing import Any, Dict, Optional, cast, Tuple
from typing import Sequence
from uuid import UUID
import httpx
import urllib.parse
from overrides import override

from chromadb.api.base_http_client import BaseHTTPClient
from chromadb.types import Database, Tenant
import chromadb.utils.embedding_functions as ef
from chromadb.api import ServerAPI
from chromadb.api.models.Collection import Collection
from chromadb.api.types import (
    DataLoader,
    Documents,
    Embeddable,
    Embeddings,
    EmbeddingFunction,
    IDs,
    Include,
    Loadable,
    Metadatas,
    URIs,
    Where,
    WhereDocument,
    GetResult,
    QueryResult,
    CollectionMetadata,
    validate_batch,
)
from chromadb.auth import (
    ClientAuthProvider,
)
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.telemetry.product import ProductTelemetryClient
from chromadb.types import Collection as CollectionModel

logger = logging.getLogger(__name__)


class FastAPI(BaseHTTPClient, ServerAPI):
    def __init__(self, system: System):
        super().__init__(system)
        system.settings.require("chroma_server_host")
        system.settings.require("chroma_server_http_port")

        self._opentelemetry_client = self.require(OpenTelemetryClient)
        self._product_telemetry_client = self.require(ProductTelemetryClient)
        self._settings = system.settings

        self._api_url = FastAPI.resolve_url(
            chroma_server_host=str(system.settings.chroma_server_host),
            chroma_server_http_port=system.settings.chroma_server_http_port,
            chroma_server_ssl_enabled=system.settings.chroma_server_ssl_enabled,
            default_api_path=system.settings.chroma_server_api_default_path,
        )

        self._session = httpx.Client(timeout=None)

        self._header = system.settings.chroma_server_headers
        if self._header is not None:
            self._session.headers.update(self._header)
        if self._settings.chroma_server_ssl_verify is not None:
            self._session = httpx.Client(verify=self._settings.chroma_server_ssl_verify)

        if system.settings.chroma_client_auth_provider:
            self._auth_provider = self.require(ClientAuthProvider)
            _headers = self._auth_provider.authenticate()
            for header, value in _headers.items():
                self._session.headers[header] = value.get_secret_value()

    def _make_request(self, method: str, path: str, **kwargs: Dict[str, Any]) -> Any:
        # Unlike requests, httpx does not automatically escape the path
        escaped_path = urllib.parse.quote(path, safe="/", encoding=None, errors=None)
        url = self._api_url + escaped_path

        response = self._session.request(method, url, **cast(Any, kwargs))
        BaseHTTPClient._raise_chroma_error(response)
        return json.loads(response.text)

    @trace_method("FastAPI.heartbeat", OpenTelemetryGranularity.OPERATION)
    @override
    def heartbeat(self) -> int:
        """Returns the current server time in nanoseconds to check if the server is alive"""
        resp_json = self._make_request("get", "/heartbeat")
        return int(resp_json["nanosecond heartbeat"])

    @trace_method("FastAPI.create_database", OpenTelemetryGranularity.OPERATION)
    @override
    def create_database(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
    ) -> None:
        """Creates a database"""
        self._make_request(
            "post",
            "/databases",
            json={"name": name},
            params={"tenant": tenant},
        )

    @trace_method("FastAPI.get_database", OpenTelemetryGranularity.OPERATION)
    @override
    def get_database(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
    ) -> Database:
        """Returns a database"""
        resp_json = self._make_request(
            "get",
            "/databases/" + name,
            params={"tenant": tenant},
        )
        return Database(
            id=resp_json["id"], name=resp_json["name"], tenant=resp_json["tenant"]
        )

    @trace_method("FastAPI.create_tenant", OpenTelemetryGranularity.OPERATION)
    @override
    def create_tenant(self, name: str) -> None:
        self._make_request("post", "/tenants", json={"name": name})

    @trace_method("FastAPI.get_tenant", OpenTelemetryGranularity.OPERATION)
    @override
    def get_tenant(self, name: str) -> Tenant:
        resp_json = self._make_request("get", "/tenants/" + name)
        return Tenant(name=resp_json["name"])

    @trace_method("FastAPI.list_collections", OpenTelemetryGranularity.OPERATION)
    @override
    def list_collections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[Collection]:
        """Returns a list of all collections"""
        json_collections = self._make_request(
            "get",
            "/collections",
            params=BaseHTTPClient._clean_params(
                {
                    "tenant": tenant,
                    "database": database,
                    "limit": limit,
                    "offset": offset,
                }
            ),
        )
        collections = []
        for json_collection in json_collections:
            model = CollectionModel(
                id=json_collection["id"],
                name=json_collection["name"],
                metadata=json_collection["metadata"],
                dimension=json_collection["dimension"],
                tenant=json_collection["tenant"],
                database=json_collection["database"],
                version=json_collection["version"],
            )
            collections.append(Collection(self, model=model))

        return collections

    @trace_method("FastAPI.count_collections", OpenTelemetryGranularity.OPERATION)
    @override
    def count_collections(
        self, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> int:
        """Returns a count of collections"""
        resp_json = self._make_request(
            "get",
            "/count_collections",
            params={"tenant": tenant, "database": database},
        )
        return cast(int, resp_json)

    @trace_method("FastAPI.create_collection", OpenTelemetryGranularity.OPERATION)
    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
        get_or_create: bool = False,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Collection:
        """Creates a collection"""
        resp_json = self._make_request(
            "post",
            "/collections",
            json={
                "name": name,
                "metadata": metadata,
                "get_or_create": get_or_create,
            },
            params={"tenant": tenant, "database": database},
        )

        model = CollectionModel(
            id=resp_json["id"],
            name=resp_json["name"],
            metadata=resp_json["metadata"],
            dimension=resp_json["dimension"] if "dimension" in resp_json else None,
            tenant=resp_json["tenant"],
            database=resp_json["database"],
            version=resp_json["version"] if "version" in resp_json else None,
        )
        return Collection(
            client=self,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @trace_method("FastAPI.get_collection", OpenTelemetryGranularity.OPERATION)
    @override
    def get_collection(
        self,
        name: str,
        id: Optional[UUID] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Collection:
        """Returns a collection"""
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("Name or id must be specified, but not both")

        _params = {"tenant": tenant, "database": database}
        if id is not None:
            _params["type"] = str(id)

        resp_json = self._make_request(
            "get",
            "/collections/" + name if name else str(id),
            params=_params,
        )

        model = CollectionModel(
            id=resp_json["id"],
            name=resp_json["name"],
            metadata=resp_json["metadata"],
            dimension=resp_json["dimension"],
            tenant=resp_json["tenant"],
            database=resp_json["database"],
            version=resp_json["version"],
        )
        return Collection(
            client=self,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @trace_method(
        "FastAPI.get_or_create_collection", OpenTelemetryGranularity.OPERATION
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
        return self.create_collection(
            name=name,
            metadata=metadata,
            embedding_function=embedding_function,
            data_loader=data_loader,
            get_or_create=True,
            tenant=tenant,
            database=database,
        )

    @trace_method("FastAPI._modify", OpenTelemetryGranularity.OPERATION)
    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        """Updates a collection"""
        self._make_request(
            "put",
            "/collections/" + str(id),
            json={"new_metadata": new_metadata, "new_name": new_name},
        )

    @trace_method("FastAPI.delete_collection", OpenTelemetryGranularity.OPERATION)
    @override
    def delete_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        """Deletes a collection"""
        self._make_request(
            "delete",
            "/collections/" + name,
            params={"tenant": tenant, "database": database},
        )

    @trace_method("FastAPI._count", OpenTelemetryGranularity.OPERATION)
    @override
    def _count(
        self,
        collection_id: UUID,
    ) -> int:
        """Returns the number of embeddings in the database"""
        resp_json = self._make_request(
            "get",
            "/collections/" + str(collection_id) + "/count",
        )
        return cast(int, resp_json)

    @trace_method("FastAPI._peek", OpenTelemetryGranularity.OPERATION)
    @override
    def _peek(
        self,
        collection_id: UUID,
        n: int = 10,
    ) -> GetResult:
        return cast(
            GetResult,
            self._get(
                collection_id,
                limit=n,
                include=["embeddings", "documents", "metadatas"],
            ),
        )

    @trace_method("FastAPI._get", OpenTelemetryGranularity.OPERATION)
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
        include: Include = ["metadatas", "documents"],
    ) -> GetResult:
        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        resp_json = self._make_request(
            "post",
            "/collections/" + str(collection_id) + "/get",
            json={
                "ids": ids,
                "where": where,
                "sort": sort,
                "limit": limit,
                "offset": offset,
                "where_document": where_document,
                "include": include,
            },
        )

        return GetResult(
            ids=resp_json["ids"],
            embeddings=resp_json.get("embeddings", None),
            metadatas=resp_json.get("metadatas", None),
            documents=resp_json.get("documents", None),
            data=None,
            uris=resp_json.get("uris", None),
            included=resp_json["included"],
        )

    @trace_method("FastAPI._delete", OpenTelemetryGranularity.OPERATION)
    @override
    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
    ) -> IDs:
        """Deletes embeddings from the database"""
        resp_json = self._make_request(
            "post",
            "/collections/" + str(collection_id) + "/delete",
            json={
                "ids": ids,
                "where": where,
                "where_document": where_document,
            },
        )
        return cast(IDs, resp_json)

    @trace_method("FastAPI._submit_batch", OpenTelemetryGranularity.ALL)
    def _submit_batch(
        self,
        batch: Tuple[
            IDs,
            Optional[Embeddings],
            Optional[Metadatas],
            Optional[Documents],
            Optional[URIs],
        ],
        url: str,
    ) -> None:
        """
        Submits a batch of embeddings to the database
        """
        self._make_request(
            "post",
            url,
            json={
                "ids": batch[0],
                "embeddings": batch[1],
                "metadatas": batch[2],
                "documents": batch[3],
                "uris": batch[4],
            },
        )

    @trace_method("FastAPI._add", OpenTelemetryGranularity.ALL)
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
        """
        Adds a batch of embeddings to the database
        - pass in column oriented data lists
        """
        batch = (ids, embeddings, metadatas, documents, uris)
        validate_batch(batch, {"max_batch_size": self.get_max_batch_size()})
        self._submit_batch(batch, "/collections/" + str(collection_id) + "/add")
        return True

    @trace_method("FastAPI._update", OpenTelemetryGranularity.ALL)
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
        """
        Updates a batch of embeddings in the database
        - pass in column oriented data lists
        """
        batch = (ids, embeddings, metadatas, documents, uris)
        validate_batch(batch, {"max_batch_size": self.get_max_batch_size()})
        self._submit_batch(batch, "/collections/" + str(collection_id) + "/update")
        return True

    @trace_method("FastAPI._upsert", OpenTelemetryGranularity.ALL)
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
        """
        Upserts a batch of embeddings in the database
        - pass in column oriented data lists
        """
        batch = (ids, embeddings, metadatas, documents, uris)
        validate_batch(batch, {"max_batch_size": self.get_max_batch_size()})
        self._submit_batch(batch, "/collections/" + str(collection_id) + "/upsert")
        return True

    @trace_method("FastAPI._query", OpenTelemetryGranularity.ALL)
    @override
    def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
        include: Include = ["metadatas", "documents", "distances"],
    ) -> QueryResult:
        """Gets the nearest neighbors of a single embedding"""
        resp_json = self._make_request(
            "post",
            "/collections/" + str(collection_id) + "/query",
            json={
                "query_embeddings": query_embeddings,
                "n_results": n_results,
                "where": where,
                "where_document": where_document,
                "include": include,
            },
        )

        return QueryResult(
            ids=resp_json["ids"],
            distances=resp_json.get("distances", None),
            embeddings=resp_json.get("embeddings", None),
            metadatas=resp_json.get("metadatas", None),
            documents=resp_json.get("documents", None),
            uris=resp_json.get("uris", None),
            data=None,
            included=resp_json["included"],
        )

    @trace_method("FastAPI.reset", OpenTelemetryGranularity.ALL)
    @override
    def reset(self) -> bool:
        """Resets the database"""
        resp_json = self._make_request("post", "/reset")
        return cast(bool, resp_json)

    @trace_method("FastAPI.get_version", OpenTelemetryGranularity.OPERATION)
    @override
    def get_version(self) -> str:
        """Returns the version of the server"""
        resp_json = self._make_request("get", "/version")
        return cast(str, resp_json)

    @override
    def get_settings(self) -> Settings:
        """Returns the settings of the client"""
        return self._settings

    @trace_method("FastAPI.get_max_batch_size", OpenTelemetryGranularity.OPERATION)
    @override
    def get_max_batch_size(self) -> int:
        if self._max_batch_size == -1:
            resp_json = self._make_request("get", "/pre-flight-checks")
            self._max_batch_size = cast(int, resp_json["max_batch_size"])
        return self._max_batch_size
