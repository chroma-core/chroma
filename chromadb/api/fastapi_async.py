from uuid import UUID
import orjson as json
from typing import Optional, cast, Tuple, Sequence, TypeVar, Callable, ParamSpec
import logging
from urllib.parse import quote, urlparse, urlunparse
import aiohttp
from overrides import override
from chromadb.api import ServerAPI
from chromadb.api.types import CollectionMetadata, EmbeddingFunction, GetResult
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, System, Settings
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.telemetry.product import ProductTelemetryClient
import chromadb.utils.embedding_functions as ef

from chromadb.types import Database, Tenant
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

logger = logging.getLogger(__name__)


class FastAPIAsync(ServerAPI):
    def __init__(self, system: System):
        super().__init__(system)

        system.settings.require("chroma_server_host")
        system.settings.require("chroma_server_http_port")

        self._opentelemetry_client = self.require(OpenTelemetryClient)
        self._product_telemetry_client = self.require(ProductTelemetryClient)
        self._settings = system.settings

        self._api_url = FastAPIAsync.resolve_url(
            chroma_server_host=str(system.settings.chroma_server_host),
            chroma_server_http_port=system.settings.chroma_server_http_port,
            chroma_server_ssl_enabled=system.settings.chroma_server_ssl_enabled,
            default_api_path=system.settings.chroma_server_api_default_path,
        )

        self._session = None

    async def _make_request(self, method: str, url: str, **kwargs):
        if self._session is None:
            # todo: better error
            raise ValueError("Session not initialized")

        async with self._session.request(method, url, **kwargs) as response:
            raise_chroma_error(response)
            return json.loads(await response.text())

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self._session is not None:
            await self._session.close()

    @staticmethod
    def _validate_host(host: str) -> None:
        parsed = urlparse(host)
        if "/" in host and parsed.scheme not in {"http", "https"}:
            raise ValueError(
                "Invalid URL. " f"Unrecognized protocol - {parsed.scheme}."
            )
        if "/" in host and (not host.startswith("http")):
            raise ValueError(
                "Invalid URL. "
                "Seems that you are trying to pass URL as a host but without \
                  specifying the protocol. "
                "Please add http:// or https:// to the host."
            )

    @staticmethod
    def resolve_url(
        chroma_server_host: str,
        chroma_server_ssl_enabled: Optional[bool] = False,
        default_api_path: Optional[str] = "",
        chroma_server_http_port: Optional[int] = 8000,
    ) -> str:
        _skip_port = False
        _chroma_server_host = chroma_server_host
        FastAPIAsync._validate_host(_chroma_server_host)
        if _chroma_server_host.startswith("http"):
            logger.debug("Skipping port as the user is passing a full URL")
            _skip_port = True
        parsed = urlparse(_chroma_server_host)

        scheme = "https" if chroma_server_ssl_enabled else parsed.scheme or "http"
        net_loc = parsed.netloc or parsed.hostname or chroma_server_host
        port = (
            ":" + str(parsed.port or chroma_server_http_port) if not _skip_port else ""
        )
        path = parsed.path or default_api_path

        if not path or path == net_loc:
            path = default_api_path if default_api_path else ""
        if not path.endswith(default_api_path or ""):
            path = path + default_api_path if default_api_path else ""
        full_url = urlunparse(
            (scheme, f"{net_loc}{port}", quote(path.replace("//", "/")), "", "", "")
        )

        return full_url

    @trace_method("FastAPI.heartbeat", OpenTelemetryGranularity.OPERATION)
    @override
    async def heartbeat(self) -> int:
        response = await self._make_request("get", self._api_url)
        return int(response["nanosecond heartbeat"])

    @trace_method("FastAPI.create_database", OpenTelemetryGranularity.OPERATION)
    @override
    async def create_database(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
    ) -> None:
        await self._make_request(
            "post",
            self._api_url + "/databases",
            data=json.dumps({"name": name}),
            params={"tenant": tenant},
        )

    @trace_method("FastAPI.get_database", OpenTelemetryGranularity.OPERATION)
    @override
    async def get_database(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
    ) -> Database:
        response = await self._make_request(
            "get",
            self._api_url + "/databases/" + name,
            params={"tenant": tenant},
        )

        return Database(
            id=response["id"], name=response["name"], tenant=response["tenant"]
        )

    @trace_method("FastAPI.create_tenant", OpenTelemetryGranularity.OPERATION)
    @override
    async def create_tenant(self, name: str) -> None:
        """Creates a tenant"""
        # resp = self._session.post(
        #     self._api_url + "/tenants",
        #     data=json.dumps({"name": name}),
        # )
        # raise_chroma_error(resp)

    @trace_method("FastAPI.get_tenant", OpenTelemetryGranularity.OPERATION)
    @override
    async def get_tenant(self, name: str) -> Tenant:
        """Returns a tenant"""
        # resp = self._session.get(
        #     self._api_url + "/tenants/" + name,
        # )
        # raise_chroma_error(resp)
        # resp_json = json.loads(resp.text)
        # return Tenant(name=resp_json["name"])

    @trace_method("FastAPI.list_collections", OpenTelemetryGranularity.OPERATION)
    @override
    async def list_collections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[Collection]:
        """Returns a list of all collections"""
        # resp = self._session.get(
        #     self._api_url + "/collections",
        #     params={
        #         "tenant": tenant,
        #         "database": database,
        #         "limit": limit,
        #         "offset": offset,
        #     },
        # )
        # raise_chroma_error(resp)
        # json_collections = json.loads(resp.text)
        # collections = []
        # for json_collection in json_collections:
        #     collections.append(Collection(self, **json_collection))

        # return collections

    @trace_method("FastAPI.count_collections", OpenTelemetryGranularity.OPERATION)
    @override
    async def count_collections(
        self, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> int:
        """Returns a count of collections"""
        # resp = self._session.get(
        #     self._api_url + "/count_collections",
        #     params={"tenant": tenant, "database": database},
        # )
        # raise_chroma_error(resp)
        # return cast(int, json.loads(resp.text))

    @trace_method("FastAPI.create_collection", OpenTelemetryGranularity.OPERATION)
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
    ) -> Collection:
        """Creates a collection"""
        # resp = self._session.post(
        #     self._api_url + "/collections",
        #     data=json.dumps(
        #         {
        #             "name": name,
        #             "metadata": metadata,
        #             "get_or_create": get_or_create,
        #         }
        #     ),
        #     params={"tenant": tenant, "database": database},
        # )
        # raise_chroma_error(resp)
        # resp_json = json.loads(resp.text)
        # return Collection(
        #     client=self,
        #     id=resp_json["id"],
        #     name=resp_json["name"],
        #     embedding_function=embedding_function,
        #     data_loader=data_loader,
        #     metadata=resp_json["metadata"],
        # )

    @trace_method("FastAPI.get_collection", OpenTelemetryGranularity.OPERATION)
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
    ) -> Collection:
        """Returns a collection"""
        # if (name is None and id is None) or (name is not None and id is not None):
        #     raise ValueError("Name or id must be specified, but not both")

        # _params = {"tenant": tenant, "database": database}
        # if id is not None:
        #     _params["type"] = str(id)
        # resp = self._session.get(
        #     self._api_url + "/collections/" + name if name else str(id), params=_params
        # )
        # raise_chroma_error(resp)
        # resp_json = json.loads(resp.text)
        # return Collection(
        #     client=self,
        #     name=resp_json["name"],
        #     id=resp_json["id"],
        #     embedding_function=embedding_function,
        #     data_loader=data_loader,
        #     metadata=resp_json["metadata"],
        # )

    @trace_method(
        "FastAPI.get_or_create_collection", OpenTelemetryGranularity.OPERATION
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
    ) -> Collection:
        "sodfihsdof"
        # return cast(
        #     Collection,
        #     self.create_collection(
        #         name=name,
        #         metadata=metadata,
        #         embedding_function=embedding_function,
        #         data_loader=data_loader,
        #         get_or_create=True,
        #         tenant=tenant,
        #         database=database,
        #     ),
        # )

    @trace_method("FastAPI._modify", OpenTelemetryGranularity.OPERATION)
    @override
    async def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        """Updates a collection"""
        # resp = self._session.put(
        #     self._api_url + "/collections/" + str(id),
        #     data=json.dumps({"new_metadata": new_metadata, "new_name": new_name}),
        # )
        # raise_chroma_error(resp)

    @trace_method("FastAPI.delete_collection", OpenTelemetryGranularity.OPERATION)
    @override
    async def delete_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        """Deletes a collection"""
        # resp = self._session.delete(
        #     self._api_url + "/collections/" + name,
        #     params={"tenant": tenant, "database": database},
        # )
        # raise_chroma_error(resp)

    @trace_method("FastAPI._count", OpenTelemetryGranularity.OPERATION)
    @override
    async def _count(
        self,
        collection_id: UUID,
    ) -> int:
        """Returns the number of embeddings in the database"""
        # resp = self._session.get(
        #     self._api_url + "/collections/" + str(collection_id) + "/count"
        # )
        # raise_chroma_error(resp)
        # return cast(int, json.loads(resp.text))

    @trace_method("FastAPI._peek", OpenTelemetryGranularity.OPERATION)
    @override
    async def _peek(
        self,
        collection_id: UUID,
        n: int = 10,
    ) -> GetResult:
        "oasidhiosdf"
        # return cast(
        #     GetResult,
        #     self._get(
        #         collection_id,
        #         limit=n,
        #         include=["embeddings", "documents", "metadatas"],
        #     ),
        # )

    @trace_method("FastAPI._get", OpenTelemetryGranularity.OPERATION)
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
        "asodifhsdf"
        # if page and page_size:
        #     offset = (page - 1) * page_size
        #     limit = page_size

        # resp = self._session.post(
        #     self._api_url + "/collections/" + str(collection_id) + "/get",
        #     data=json.dumps(
        #         {
        #             "ids": ids,
        #             "where": where,
        #             "sort": sort,
        #             "limit": limit,
        #             "offset": offset,
        #             "where_document": where_document,
        #             "include": include,
        #         }
        #     ),
        # )

        # raise_chroma_error(resp)
        # body = json.loads(resp.text)
        # return GetResult(
        #     ids=body["ids"],
        #     embeddings=body.get("embeddings", None),
        #     metadatas=body.get("metadatas", None),
        #     documents=body.get("documents", None),
        #     data=None,
        #     uris=body.get("uris", None),
        #     included=body["included"],
        # )

    @trace_method("FastAPI._delete", OpenTelemetryGranularity.OPERATION)
    @override
    async def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
    ) -> IDs:
        """Deletes embeddings from the database"""
        # resp = self._session.post(
        #     self._api_url + "/collections/" + str(collection_id) + "/delete",
        #     data=json.dumps(
        #         {"where": where, "ids": ids, "where_document": where_document}
        #     ),
        # )

        # raise_chroma_error(resp)
        # return cast(IDs, json.loads(resp.text))

    @trace_method("FastAPI._submit_batch", OpenTelemetryGranularity.ALL)
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
    ):  # -> requests.Response:
        """
        Submits a batch of embeddings to the database
        """
        # resp = self._session.post(
        #     self._api_url + url,
        #     data=json.dumps(
        #         {
        #             "ids": batch[0],
        #             "embeddings": batch[1],
        #             "metadatas": batch[2],
        #             "documents": batch[3],
        #             "uris": batch[4],
        #         }
        #     ),
        # )
        # return resp

    @trace_method("FastAPI._add", OpenTelemetryGranularity.ALL)
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
        # batch = (ids, embeddings, metadatas, documents, uris)
        # validate_batch(batch, {"max_batch_size": self.max_batch_size})
        # resp = self._submit_batch(batch, "/collections/" + str(collection_id) + "/add")
        # raise_chroma_error(resp)
        # return True

    @trace_method("FastAPI._update", OpenTelemetryGranularity.ALL)
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
        """
        Updates a batch of embeddings in the database
        - pass in column oriented data lists
        """
        # batch = (ids, embeddings, metadatas, documents, uris)
        # validate_batch(batch, {"max_batch_size": self.max_batch_size})
        # resp = self._submit_batch(
        #     batch, "/collections/" + str(collection_id) + "/update"
        # )
        # raise_chroma_error(resp)
        # return True

    @trace_method("FastAPI._upsert", OpenTelemetryGranularity.ALL)
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
        """
        Upserts a batch of embeddings in the database
        - pass in column oriented data lists
        """
        # batch = (ids, embeddings, metadatas, documents, uris)
        # validate_batch(batch, {"max_batch_size": self.max_batch_size})
        # resp = self._submit_batch(
        #     batch, "/collections/" + str(collection_id) + "/upsert"
        # )
        # raise_chroma_error(resp)
        # return True

    @trace_method("FastAPI._query", OpenTelemetryGranularity.ALL)
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
        """Gets the nearest neighbors of a single embedding"""
        # resp = self._session.post(
        #     self._api_url + "/collections/" + str(collection_id) + "/query",
        #     data=json.dumps(
        #         {
        #             "query_embeddings": query_embeddings,
        #             "n_results": n_results,
        #             "where": where,
        #             "where_document": where_document,
        #             "include": include,
        #         }
        #     ),
        # )

        # raise_chroma_error(resp)
        # body = json.loads(resp.text)

        # return QueryResult(
        #     ids=body["ids"],
        #     distances=body.get("distances", None),
        #     embeddings=body.get("embeddings", None),
        #     metadatas=body.get("metadatas", None),
        #     documents=body.get("documents", None),
        #     uris=body.get("uris", None),
        #     data=None,
        #     included=body["included"],
        # )

    @trace_method("FastAPI.reset", OpenTelemetryGranularity.ALL)
    @override
    async def reset(self) -> bool:
        """Resets the database"""
        # resp = self._session.post(self._api_url + "/reset")
        # raise_chroma_error(resp)
        # return cast(bool, json.loads(resp.text))

    @trace_method("FastAPI.get_version", OpenTelemetryGranularity.OPERATION)
    @override
    async def get_version(self) -> str:
        """Returns the version of the server"""
        # resp = self._session.get(self._api_url + "/version")
        # raise_chroma_error(resp)
        # return cast(str, json.loads(resp.text))

    @override
    async def get_settings(self) -> Settings:
        """Returns the settings of the client"""
        # return self._settings

    @property
    @trace_method("FastAPI.max_batch_size", OpenTelemetryGranularity.OPERATION)
    @override
    async def max_batch_size(self) -> int:
        "asdfoih"
        # if self._max_batch_size == -1:
        #     resp = self._session.get(self._api_url + "/pre-flight-checks")
        #     raise_chroma_error(resp)
        #     self._max_batch_size = cast(int, json.loads(resp.text)["max_batch_size"])
        # return self._max_batch_size


def raise_chroma_error(resp: aiohttp.ClientResponse) -> None:
    """Raises an error if the response is not ok, using a ChromaError if possible"""
    if resp.ok:
        return

    # chroma_error = None
    # try:
    #     body = json.loads(resp.text)
    #     if "error" in body:
    #         if body["error"] in errors.error_types:
    #             chroma_error = errors.error_types[body["error"]](body["message"])

    # except BaseException:
    #     pass

    # if chroma_error:
    #     raise chroma_error

    # try:
    #     resp.raise_for_status()
    # except requests.HTTPError:
    #     raise (Exception(resp.text))