import asyncio
from uuid import UUID
import urllib.parse
import orjson as json
from typing import Any, Awaitable, Optional, TypeVar, cast, Tuple, Sequence, Dict
import logging
import httpx
from overrides import override
from chromadb import errors
from chromadb.api import AsyncServerAPI
from chromadb.api.base_http_client import BaseHTTPClient
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, System, Settings
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.telemetry.product import ProductTelemetryClient
from chromadb.utils.async_to_sync import async_class_to_sync
import chromadb.utils.embedding_functions as ef

from chromadb.types import Database, Tenant
from chromadb.types import Collection as CollectionModel

from chromadb.api.models.AsyncCollection import AsyncCollection
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


# requests removes None values from the built query string, but httpx includes it as an empty value
T = TypeVar("T", bound=dict[Any, Any])


def clean_params(params: T) -> T:
    """Remove None values from kwargs."""
    return {k: v for k, v in params.items() if v is not None}  # type: ignore


logger = logging.getLogger(__name__)


class AsyncFastAPI(BaseHTTPClient, AsyncServerAPI):
    # We make one client per event loop to avoid unexpected issues if a client
    # is shared between event loops.
    # For example, if a client is constructed in the main thread, then passed
    # (or a returned Collection is passed) to a new thread, the client would
    # normally throw an obscure asyncio error.
    # Mixing asyncio and threading in this manner usually discouraged, but
    # this gives a better user experience with practically no downsides.
    # https://github.com/encode/httpx/issues/2058
    _clients: Dict[int, httpx.AsyncClient] = {}

    def __init__(self, system: System):
        super().__init__(system)

        system.settings.require("chroma_server_host")
        system.settings.require("chroma_server_http_port")

        self._opentelemetry_client = self.require(OpenTelemetryClient)
        self._product_telemetry_client = self.require(ProductTelemetryClient)
        self._settings = system.settings

        self._api_url = AsyncFastAPI.resolve_url(
            chroma_server_host=str(system.settings.chroma_server_host),
            chroma_server_http_port=system.settings.chroma_server_http_port,
            chroma_server_ssl_enabled=system.settings.chroma_server_ssl_enabled,
            default_api_path=system.settings.chroma_server_api_default_path,
        )

    async def __aenter__(self) -> "AsyncFastAPI":
        self._get_client()
        return self

    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        await self.stop()

    @override
    # todo: syncify?
    async def stop(self):
        super().stop()
        for client in self._clients.values():
            await client.aclose()
            del client

    def _get_client(self) -> httpx.AsyncClient:
        # Ideally this would use anyio to be compatible with both
        # asyncio and trio, but anyio does not expose any way to identify
        # the current event loop.
        # We attempt to get the loop assuming the environment is asyncio, and
        # otherwise gracefully fall back to using a singleton client.
        loop_hash = None
        try:
            loop = asyncio.get_event_loop()
            loop_hash = loop.__hash__()
        except RuntimeError:
            loop_hash = 0

        if loop_hash not in self._clients:
            self._clients[loop_hash] = httpx.AsyncClient(timeout=None)

        return self._clients[loop_hash]

    async def _make_request(
        self, method: str, path: str, **kwargs: dict[str, Any]
    ) -> Any:
        # Unlike requests, httpx does not automatically escape the path
        escaped_path = urllib.parse.quote(path, safe="/", encoding=None, errors=None)
        url = self._api_url + escaped_path

        response = await self._get_client().request(method, url, **kwargs)
        await raise_chroma_error(response)
        return json.loads(response.text)

    @trace_method("AsyncFastAPI.heartbeat", OpenTelemetryGranularity.OPERATION)
    @override
    async def heartbeat(self) -> int:
        response = await self._make_request("get", "")
        return int(response["nanosecond heartbeat"])

    @trace_method("AsyncFastAPI.create_database", OpenTelemetryGranularity.OPERATION)
    @override
    async def create_database(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
    ) -> None:
        await self._make_request(
            "post",
            "/databases",
            json={"name": name},
            params={"tenant": tenant},
        )

    @trace_method("AsyncFastAPI.get_database", OpenTelemetryGranularity.OPERATION)
    @override
    async def get_database(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
    ) -> Database:
        response = await self._make_request(
            "get",
            "/databases/" + name,
            params={"tenant": tenant},
        )

        return Database(
            id=response["id"], name=response["name"], tenant=response["tenant"]
        )

    @trace_method("AsyncFastAPI.create_tenant", OpenTelemetryGranularity.OPERATION)
    @override
    async def create_tenant(self, name: str) -> None:
        await self._make_request(
            "post",
            "/tenants",
            json={"name": name},
        )

    @trace_method("AsyncFastAPI.get_tenant", OpenTelemetryGranularity.OPERATION)
    @override
    async def get_tenant(self, name: str) -> Tenant:
        resp_json = await self._make_request(
            "get",
            "/tenants/" + name,
        )

        return Tenant(name=resp_json["name"])

    @trace_method("AsyncFastAPI.list_collections", OpenTelemetryGranularity.OPERATION)
    @override
    async def list_collections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[AsyncCollection]:
        resp_json = await self._make_request(
            "get",
            "/collections",
            params=clean_params(
                {
                    "tenant": tenant,
                    "database": database,
                    "limit": limit,
                    "offset": offset,
                }
            ),
        )

        collections = []
        for json_collection in resp_json:
            model = CollectionModel(
                id=json_collection["id"],
                name=json_collection["name"],
                metadata=json_collection["metadata"],
                dimension=json_collection["dimension"],
                tenant=json_collection["tenant"],
                database=json_collection["database"],
                version=json_collection["version"],
            )

            collections.append(AsyncCollection(client=self, model=model))

        return collections

    @trace_method("AsyncFastAPI.count_collections", OpenTelemetryGranularity.OPERATION)
    @override
    async def count_collections(
        self, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> int:
        resp_json = await self._make_request(
            "get",
            "/count_collections",
            params={"tenant": tenant, "database": database},
        )

        return cast(int, resp_json)

    @trace_method("AsyncFastAPI.create_collection", OpenTelemetryGranularity.OPERATION)
    @override
    async def create_collection(
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
    ) -> AsyncCollection:
        """Creates a collection"""
        resp_json = await self._make_request(
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
            dimension=resp_json["dimension"],
            tenant=resp_json["tenant"],
            database=resp_json["database"],
            version=resp_json["version"],
        )

        return AsyncCollection(
            client=self,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @trace_method("AsyncFastAPI.get_collection", OpenTelemetryGranularity.OPERATION)
    @override
    async def get_collection(
        self,
        name: str,
        id: Optional[UUID] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> AsyncCollection:
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("Name or id must be specified, but not both")

        params = {"tenant": tenant, "database": database}
        if id is not None:
            params["type"] = str(id)

        resp_json = await self._make_request(
            "get",
            "/collections/" + name if name else str(id),
            params=params,
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

        return AsyncCollection(
            client=self,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @trace_method(
        "AsyncFastAPI.get_or_create_collection", OpenTelemetryGranularity.OPERATION
    )
    @override
    async def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> AsyncCollection:
        return await self.create_collection(
            name=name,
            metadata=metadata,
            embedding_function=embedding_function,
            data_loader=data_loader,
            get_or_create=True,
            tenant=tenant,
            database=database,
        )

    @trace_method("AsyncFastAPI._modify", OpenTelemetryGranularity.OPERATION)
    @override
    async def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        await self._make_request(
            "put",
            "/collections/" + str(id),
            json={"new_metadata": new_metadata, "new_name": new_name},
        )

    @trace_method("AsyncFastAPI.delete_collection", OpenTelemetryGranularity.OPERATION)
    @override
    async def delete_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        await self._make_request(
            "delete",
            "/collections/" + name,
            params={"tenant": tenant, "database": database},
        )

    @trace_method("AsyncFastAPI._count", OpenTelemetryGranularity.OPERATION)
    @override
    async def _count(
        self,
        collection_id: UUID,
    ) -> int:
        """Returns the number of embeddings in the database"""
        resp_json = await self._make_request(
            "get",
            "/collections/" + str(collection_id) + "/count",
        )

        return cast(int, resp_json)

    @trace_method("AsyncFastAPI._peek", OpenTelemetryGranularity.OPERATION)
    @override
    async def _peek(
        self,
        collection_id: UUID,
        n: int = 10,
    ) -> GetResult:
        return await self._get(
            collection_id,
            limit=n,
            include=["embeddings", "documents", "metadatas"],
        )

    @trace_method("AsyncFastAPI._get", OpenTelemetryGranularity.OPERATION)
    @override
    async def _get(
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

        resp_json = await self._make_request(
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

    @trace_method("AsyncFastAPI._delete", OpenTelemetryGranularity.OPERATION)
    @override
    async def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
    ) -> IDs:
        resp_json = await self._make_request(
            "post",
            "/collections/" + str(collection_id) + "/delete",
            json={"where": where, "ids": ids, "where_document": where_document},
        )

        return cast(IDs, resp_json)

    @trace_method("AsyncFastAPI._submit_batch", OpenTelemetryGranularity.ALL)
    async def _submit_batch(
        self,
        batch: Tuple[
            IDs,
            Optional[Embeddings],
            Optional[Metadatas],
            Optional[Documents],
            Optional[URIs],
        ],
        url: str,
    ) -> Any:
        """
        Submits a batch of embeddings to the database
        """
        return await self._make_request(
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

    @trace_method("AsyncFastAPI._add", OpenTelemetryGranularity.ALL)
    @override
    async def _add(
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
        validate_batch(batch, {"max_batch_size": await self.get_max_batch_size()})
        await self._submit_batch(batch, "/collections/" + str(collection_id) + "/add")
        return True

    @trace_method("AsyncFastAPI._update", OpenTelemetryGranularity.ALL)
    @override
    async def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> bool:
        batch = (ids, embeddings, metadatas, documents, uris)
        validate_batch(batch, {"max_batch_size": await self.get_max_batch_size()})

        await self._submit_batch(
            batch, "/collections/" + str(collection_id) + "/update"
        )

        return True

    @trace_method("AsyncFastAPI._upsert", OpenTelemetryGranularity.ALL)
    @override
    async def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> bool:
        batch = (ids, embeddings, metadatas, documents, uris)
        validate_batch(batch, {"max_batch_size": await self.get_max_batch_size()})
        await self._submit_batch(
            batch, "/collections/" + str(collection_id) + "/upsert"
        )
        return True

    @trace_method("AsyncFastAPI._query", OpenTelemetryGranularity.ALL)
    @override
    async def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
        include: Include = ["metadatas", "documents", "distances"],
    ) -> QueryResult:
        resp_json = await self._make_request(
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

    @trace_method("AsyncFastAPI.reset", OpenTelemetryGranularity.ALL)
    @override
    async def reset(self) -> bool:
        resp_json = await self._make_request("post", "/reset")
        return cast(bool, resp_json)

    @trace_method("AsyncFastAPI.get_version", OpenTelemetryGranularity.OPERATION)
    @override
    async def get_version(self) -> str:
        resp_json = await self._make_request("get", "/version")
        return cast(str, resp_json)

    @override
    def get_settings(self) -> Settings:
        return self._settings

    # todo: cleanup
    @trace_method("AsyncFastAPI.get_max_batch_size", OpenTelemetryGranularity.OPERATION)
    @override
    async def get_max_batch_size(self) -> int:
        if self._max_batch_size == -1:
            resp_json = await self._make_request("get", "/pre-flight-checks")
            self._max_batch_size = cast(int, resp_json["max_batch_size"])
        return self._max_batch_size


async def raise_chroma_error(resp: httpx.Response) -> Any:
    """Raises an error if the response is not ok, using a ChromaError if possible."""
    try:
        resp.raise_for_status()
        return
    except httpx.HTTPStatusError:
        pass

    chroma_error = None
    try:
        body = json.loads(resp.text)
        if "error" in body:
            if body["error"] in errors.error_types:
                chroma_error = errors.error_types[body["error"]](body["message"])

    except BaseException:
        pass

    if chroma_error:
        raise chroma_error

    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError:
        raise (Exception(resp.text))


# todo: move to test directory?
@async_class_to_sync
class AsyncFastAPISync(AsyncFastAPI):
    pass
