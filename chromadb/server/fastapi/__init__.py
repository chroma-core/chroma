from typing import Any, Callable, cast, Dict, List, Sequence, Optional, Tuple
import fastapi
import orjson
from anyio import (
    to_thread,
    CapacityLimiter,
)
from fastapi import FastAPI as _FastAPI, Response, Request
from fastapi.responses import JSONResponse, ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi import HTTPException, status
from uuid import UUID

# from chromadb.api.models.Collection import Collection
from chromadb.api.types import GetResult, QueryResult
from chromadb.auth import (
    AuthzAction,
    AuthzResource,
    ServerAuthenticationProvider,
    ServerAuthorizationProvider,
)
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.api import ServerAPI
from chromadb.errors import (
    ChromaError,
    InvalidDimensionException,
    InvalidHTTPVersion,
)
from chromadb.quota import QuotaError
from chromadb.rate_limiting import RateLimitError
from chromadb.server import Server
from chromadb.server.fastapi.types import (
    AddEmbedding,
    CreateDatabase,
    CreateTenant,
    DeleteEmbedding,
    GetEmbedding,
    QueryEmbedding,
    CreateCollection,
    UpdateCollection,
    UpdateEmbedding,
)
from starlette.datastructures import Headers
import logging
from chromadb.telemetry.product.events import ServerStartEvent
from chromadb.utils.fastapi import fastapi_json_response, string_to_uuid as _uuid
from chromadb.telemetry.opentelemetry.fastapi import instrument_fastapi
from chromadb.types import Database, Tenant
from chromadb.telemetry.product import ServerContext, ProductTelemetryClient
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.types import Collection as CollectionModel

logger = logging.getLogger(__name__)


def use_route_names_as_operation_ids(app: _FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.
    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


async def catch_exceptions_middleware(
    request: Request, call_next: Callable[[Request], Any]
) -> Response:
    try:
        return await call_next(request)
    except ChromaError as e:
        return fastapi_json_response(e)
    except Exception as e:
        logger.exception(e)
        return JSONResponse(content={"error": repr(e)}, status_code=500)


async def check_http_version_middleware(
    request: Request, call_next: Callable[[Request], Any]
) -> Response:
    http_version = request.scope.get("http_version")
    if http_version not in ["1.1", "2"]:
        raise InvalidHTTPVersion(f"HTTP version {http_version} is not supported")
    return await call_next(request)


class ChromaAPIRouter(fastapi.APIRouter):  # type: ignore
    # A simple subclass of fastapi's APIRouter which treats URLs with a
    # trailing "/" the same as URLs without. Docs will only contain URLs
    # without trailing "/"s.
    def add_api_route(self, path: str, *args: Any, **kwargs: Any) -> None:
        # If kwargs["include_in_schema"] isn't passed OR is True, we should
        # only include the non-"/" path. If kwargs["include_in_schema"] is
        # False, include neither.
        exclude_from_schema = (
            "include_in_schema" in kwargs and not kwargs["include_in_schema"]
        )

        def include_in_schema(path: str) -> bool:
            nonlocal exclude_from_schema
            return not exclude_from_schema and not path.endswith("/")

        kwargs["include_in_schema"] = include_in_schema(path)
        super().add_api_route(path, *args, **kwargs)

        if path.endswith("/"):
            path = path[:-1]
        else:
            path = path + "/"

        kwargs["include_in_schema"] = include_in_schema(path)
        super().add_api_route(path, *args, **kwargs)


class FastAPI(Server):
    def __init__(self, settings: Settings):
        super().__init__(settings)
        ProductTelemetryClient.SERVER_CONTEXT = ServerContext.FASTAPI
        # https://fastapi.tiangolo.com/advanced/custom-response/#use-orjsonresponse
        self._app = fastapi.FastAPI(debug=True, default_response_class=ORJSONResponse)
        self._system = System(settings)
        self._api: ServerAPI = self._system.instance(ServerAPI)
        self._opentelemetry_client = self._api.require(OpenTelemetryClient)
        self._capacity_limiter = CapacityLimiter(
            settings.chroma_server_thread_pool_size
        )
        self._system.start()

        self._app.middleware("http")(check_http_version_middleware)
        self._app.middleware("http")(catch_exceptions_middleware)
        self._app.add_middleware(
            CORSMiddleware,
            allow_headers=["*"],
            allow_origins=settings.chroma_server_cors_allow_origins,
            allow_methods=["*"],
        )
        self._app.add_exception_handler(QuotaError, self.quota_exception_handler)
        self._app.add_exception_handler(
            RateLimitError, self.rate_limit_exception_handler
        )

        self._app.on_event("shutdown")(self.shutdown)

        self.authn_provider = None
        if settings.chroma_server_authn_provider:
            self.authn_provider = self._system.require(ServerAuthenticationProvider)

        self.authz_provider = None
        if settings.chroma_server_authz_provider:
            self.authz_provider = self._system.require(ServerAuthorizationProvider)

        self.router = ChromaAPIRouter()

        self.router.add_api_route("/api/v1", self.root, methods=["GET"])
        self.router.add_api_route("/api/v1/reset", self.reset, methods=["POST"])
        self.router.add_api_route("/api/v1/version", self.version, methods=["GET"])
        self.router.add_api_route("/api/v1/heartbeat", self.heartbeat, methods=["GET"])
        self.router.add_api_route(
            "/api/v1/pre-flight-checks", self.pre_flight_checks, methods=["GET"]
        )

        self.router.add_api_route(
            "/api/v1/databases",
            self.create_database,
            methods=["POST"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/databases/{database}",
            self.get_database,
            methods=["GET"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/tenants",
            self.create_tenant,
            methods=["POST"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/tenants/{tenant}",
            self.get_tenant,
            methods=["GET"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/collections",
            self.list_collections,
            methods=["GET"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/count_collections",
            self.count_collections,
            methods=["GET"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections",
            self.create_collection,
            methods=["POST"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/add",
            self.add,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/update",
            self.update,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/upsert",
            self.upsert,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/get",
            self.get,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/delete",
            self.delete,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/count",
            self.count,
            methods=["GET"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/query",
            self.get_nearest_neighbors,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}",
            self.get_collection,
            methods=["GET"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}",
            self.update_collection,
            methods=["PUT"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}",
            self.delete_collection,
            methods=["DELETE"],
            response_model=None,
        )

        self._app.include_router(self.router)

        use_route_names_as_operation_ids(self._app)
        instrument_fastapi(self._app)
        telemetry_client = self._system.instance(ProductTelemetryClient)
        telemetry_client.capture(ServerStartEvent())

    def shutdown(self) -> None:
        self._system.stop()

    def app(self) -> fastapi.FastAPI:
        return self._app

    async def rate_limit_exception_handler(
        self, request: Request, exc: RateLimitError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=429,
            content={
                "message": f"rate limit. resource: {exc.resource} quota: {exc.quota}"
            },
        )

    def root(self) -> Dict[str, int]:
        return {"nanosecond heartbeat": self._api.heartbeat()}

    async def quota_exception_handler(
        self, request: Request, exc: QuotaError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=429,
            content={
                "message": f"quota error. resource: {exc.resource} "
                f"quota: {exc.quota} actual: {exc.actual}"
            },
        )

    async def heartbeat(self) -> Dict[str, int]:
        return self.root()

    async def version(self) -> str:
        return self._api.get_version()

    def auth_and_get_tenant_and_database_for_request(
        self,
        headers: Headers,
        action: AuthzAction,
        tenant: Optional[str],
        database: Optional[str],
        collection: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Authenticates and authorizes the request based on the given headers
        and other parameters. If the request cannot be authenticated or cannot
        be authorized (with the configured providers), raises an HTTP 401.

        If the request is authenticated and authorized, returns the tenant and
        database to be used for the request. These will differ from the passed
        tenant and database if and only if:
        - The request is authenticated
        - chroma_overwrite_singleton_tenant_database_access_from_auth = True
        - The passed tenant or database are None or default_{tenant, database}
            (can be overwritten separately)
        - The user has access to a single tenant and/or single database.
        """
        if not self.authn_provider:
            return (tenant, database)

        user_identity = self.authn_provider.authenticate_or_raise(headers)

        (
            new_tenant,
            new_database,
        ) = self.authn_provider.singleton_tenant_database_if_applicable(user_identity)

        if (not tenant or tenant == DEFAULT_TENANT) and new_tenant:
            tenant = new_tenant
        if (not database or database == DEFAULT_DATABASE) and new_database:
            database = new_database

        if not self.authz_provider:
            return (tenant, database)

        authz_resource = AuthzResource(
            tenant=tenant,
            database=database,
            collection=collection,
        )

        self.authz_provider.authorize_or_raise(user_identity, action, authz_resource)
        return (tenant, database)

    @trace_method("FastAPI.create_database", OpenTelemetryGranularity.OPERATION)
    async def create_database(
        self, request: Request, tenant: str = DEFAULT_TENANT
    ) -> None:
        def process_create_database(
            tenant: str, headers: Headers, raw_body: bytes
        ) -> None:
            db = CreateDatabase.model_validate(orjson.loads(raw_body))

            (
                maybe_tenant,
                maybe_database,
            ) = self.auth_and_get_tenant_and_database_for_request(
                headers,
                AuthzAction.CREATE_DATABASE,
                tenant,
                db.name,
                None,
            )
            if maybe_tenant:
                tenant = maybe_tenant
            if maybe_database:
                db.name = maybe_database

            return self._api.create_database(db.name, tenant)

        await to_thread.run_sync(
            process_create_database,
            tenant,
            request.headers,
            await request.body(),
            limiter=self._capacity_limiter,
        )

    @trace_method("FastAPI.get_database", OpenTelemetryGranularity.OPERATION)
    async def get_database(
        self,
        request: Request,
        database: str,
        tenant: str = DEFAULT_TENANT,
    ) -> Database:
        (
            maybe_tenant,
            maybe_database,
        ) = self.auth_and_get_tenant_and_database_for_request(
            request.headers,
            AuthzAction.GET_DATABASE,
            tenant,
            database,
            None,
        )
        if maybe_tenant:
            tenant = maybe_tenant
        if maybe_database:
            database = maybe_database

        return cast(
            Database,
            await to_thread.run_sync(
                self._api.get_database,
                database,
                tenant,
                limiter=self._capacity_limiter,
            ),
        )

    @trace_method("FastAPI.create_tenant", OpenTelemetryGranularity.OPERATION)
    async def create_tenant(self, request: Request) -> None:
        def process_create_tenant(request: Request, raw_body: bytes) -> None:
            tenant = CreateTenant.model_validate(orjson.loads(raw_body))

            maybe_tenant, _ = self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.CREATE_TENANT,
                tenant.name,
                None,
                None,
            )
            if maybe_tenant:
                tenant.name = maybe_tenant

            return self._api.create_tenant(tenant.name)

        await to_thread.run_sync(
            process_create_tenant,
            request,
            await request.body(),
            limiter=self._capacity_limiter,
        )

    @trace_method("FastAPI.get_tenant", OpenTelemetryGranularity.OPERATION)
    async def get_tenant(
        self,
        request: Request,
        tenant: str,
    ) -> Tenant:
        maybe_tenant, _ = self.auth_and_get_tenant_and_database_for_request(
            request.headers,
            AuthzAction.GET_TENANT,
            tenant,
            None,
            None,
        )
        if maybe_tenant:
            tenant = maybe_tenant

        return cast(
            Tenant,
            await to_thread.run_sync(
                self._api.get_tenant,
                tenant,
                limiter=self._capacity_limiter,
            ),
        )

    @trace_method("FastAPI.list_collections", OpenTelemetryGranularity.OPERATION)
    async def list_collections(
        self,
        request: Request,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[CollectionModel]:
        (
            maybe_tenant,
            maybe_database,
        ) = self.auth_and_get_tenant_and_database_for_request(
            request.headers,
            AuthzAction.LIST_COLLECTIONS,
            tenant,
            database,
            None,
        )
        if maybe_tenant:
            tenant = maybe_tenant
        if maybe_database:
            database = maybe_database

        api_collection_models = cast(
            Sequence[CollectionModel],
            await to_thread.run_sync(
                self._api.list_collections,
                limit,
                offset,
                tenant,
                database,
                limiter=self._capacity_limiter,
            ),
        )

        return api_collection_models

    @trace_method("FastAPI.count_collections", OpenTelemetryGranularity.OPERATION)
    async def count_collections(
        self,
        request: Request,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> int:
        (
            maybe_tenant,
            maybe_database,
        ) = self.auth_and_get_tenant_and_database_for_request(
            request.headers,
            AuthzAction.COUNT_COLLECTIONS,
            tenant,
            database,
            None,
        )
        if maybe_tenant:
            tenant = maybe_tenant
        if maybe_database:
            database = maybe_database

        return cast(
            int,
            await to_thread.run_sync(
                self._api.count_collections,
                tenant,
                database,
                limiter=self._capacity_limiter,
            ),
        )

    @trace_method("FastAPI.create_collection", OpenTelemetryGranularity.OPERATION)
    async def create_collection(
        self,
        request: Request,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        def process_create_collection(
            request: Request, tenant: str, database: str, raw_body: bytes
        ) -> CollectionModel:
            create = CreateCollection.model_validate(orjson.loads(raw_body))

            (
                maybe_tenant,
                maybe_database,
            ) = self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.CREATE_COLLECTION,
                tenant,
                database,
                create.name,
            )
            if maybe_tenant:
                tenant = maybe_tenant
            if maybe_database:
                database = maybe_database

            return self._api.create_collection(
                name=create.name,
                metadata=create.metadata,
                get_or_create=create.get_or_create,
                tenant=tenant,
                database=database,
            )

        api_collection_model = cast(
            CollectionModel,
            await to_thread.run_sync(
                process_create_collection,
                request,
                tenant,
                database,
                await request.body(),
                limiter=self._capacity_limiter,
            ),
        )
        return api_collection_model

    @trace_method("FastAPI.get_collection", OpenTelemetryGranularity.OPERATION)
    async def get_collection(
        self,
        request: Request,
        collection_name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        (
            maybe_tenant,
            maybe_database,
        ) = self.auth_and_get_tenant_and_database_for_request(
            request.headers,
            AuthzAction.GET_COLLECTION,
            tenant,
            database,
            collection_name,
        )
        if maybe_tenant:
            tenant = maybe_tenant
        if maybe_database:
            database = maybe_database

        api_collection_model = cast(
            CollectionModel,
            await to_thread.run_sync(
                self._api.get_collection,
                collection_name,
                None,  # id
                tenant,
                database,
                limiter=self._capacity_limiter,
            ),
        )
        return api_collection_model

    @trace_method("FastAPI.update_collection", OpenTelemetryGranularity.OPERATION)
    async def update_collection(
        self,
        collection_id: str,
        request: Request,
    ) -> None:
        def process_update_collection(
            request: Request, collection_id: str, raw_body: bytes
        ) -> None:
            update = UpdateCollection.model_validate(orjson.loads(raw_body))
            self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.UPDATE_COLLECTION,
                None,
                None,
                collection_id,
            )
            return self._api._modify(
                id=_uuid(collection_id),
                new_name=update.new_name,
                new_metadata=update.new_metadata,
            )

        await to_thread.run_sync(
            process_update_collection,
            request,
            collection_id,
            await request.body(),
            limiter=self._capacity_limiter,
        )

    @trace_method("FastAPI.delete_collection", OpenTelemetryGranularity.OPERATION)
    async def delete_collection(
        self,
        request: Request,
        collection_name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        (
            maybe_tenant,
            maybe_database,
        ) = self.auth_and_get_tenant_and_database_for_request(
            request.headers,
            AuthzAction.DELETE_COLLECTION,
            tenant,
            database,
            collection_name,
        )
        if maybe_tenant:
            tenant = maybe_tenant
        if maybe_database:
            database = maybe_database

        await to_thread.run_sync(
            self._api.delete_collection,
            collection_name,
            tenant,
            database,
            limiter=self._capacity_limiter,
        )

    @trace_method("FastAPI.add", OpenTelemetryGranularity.OPERATION)
    async def add(self, request: Request, collection_id: str) -> bool:
        try:

            def process_add(request: Request, raw_body: bytes) -> bool:
                add = AddEmbedding.model_validate(orjson.loads(raw_body))
                self.auth_and_get_tenant_and_database_for_request(
                    request.headers,
                    AuthzAction.ADD,
                    None,
                    None,
                    collection_id,
                )
                return self._api._add(
                    collection_id=_uuid(collection_id),
                    ids=add.ids,
                    embeddings=add.embeddings,  # type: ignore
                    metadatas=add.metadatas,  # type: ignore
                    documents=add.documents,  # type: ignore
                    uris=add.uris,  # type: ignore
                )

            return cast(
                bool,
                await to_thread.run_sync(
                    process_add,
                    request,
                    await request.body(),
                    limiter=self._capacity_limiter,
                ),
            )
        except InvalidDimensionException as e:
            raise HTTPException(status_code=500, detail=str(e))

    @trace_method("FastAPI.update", OpenTelemetryGranularity.OPERATION)
    async def update(self, request: Request, collection_id: str) -> None:
        def process_update(request: Request, raw_body: bytes) -> bool:
            update = UpdateEmbedding.model_validate(orjson.loads(raw_body))

            self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.UPDATE,
                None,
                None,
                collection_id,
            )

            return self._api._update(
                collection_id=_uuid(collection_id),
                ids=update.ids,
                embeddings=update.embeddings,
                metadatas=update.metadatas,  # type: ignore
                documents=update.documents,  # type: ignore
                uris=update.uris,  # type: ignore
            )

        await to_thread.run_sync(
            process_update,
            request,
            await request.body(),
            limiter=self._capacity_limiter,
        )

    @trace_method("FastAPI.upsert", OpenTelemetryGranularity.OPERATION)
    async def upsert(self, request: Request, collection_id: str) -> None:
        def process_upsert(request: Request, raw_body: bytes) -> bool:
            upsert = AddEmbedding.model_validate(orjson.loads(raw_body))

            self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.UPSERT,
                None,
                None,
                collection_id,
            )

            return self._api._upsert(
                collection_id=_uuid(collection_id),
                ids=upsert.ids,
                embeddings=upsert.embeddings,  # type: ignore
                metadatas=upsert.metadatas,  # type: ignore
                documents=upsert.documents,  # type: ignore
                uris=upsert.uris,  # type: ignore
            )

        await to_thread.run_sync(
            process_upsert,
            request,
            await request.body(),
            limiter=self._capacity_limiter,
        )

    @trace_method("FastAPI.get", OpenTelemetryGranularity.OPERATION)
    async def get(self, collection_id: str, request: Request) -> GetResult:
        def process_get(request: Request, raw_body: bytes) -> GetResult:
            get = GetEmbedding.model_validate(orjson.loads(raw_body))
            self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.GET,
                None,
                None,
                collection_id,
            )
            return self._api._get(
                collection_id=_uuid(collection_id),
                ids=get.ids,
                where=get.where,
                sort=get.sort,
                limit=get.limit,
                offset=get.offset,
                where_document=get.where_document,
                include=get.include,
            )

        return cast(
            GetResult,
            await to_thread.run_sync(
                process_get,
                request,
                await request.body(),
                limiter=self._capacity_limiter,
            ),
        )

    @trace_method("FastAPI.delete", OpenTelemetryGranularity.OPERATION)
    async def delete(self, collection_id: str, request: Request) -> List[UUID]:
        def process_delete(request: Request, raw_body: bytes) -> List[str]:
            delete = DeleteEmbedding.model_validate(orjson.loads(raw_body))
            self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.DELETE,
                None,
                None,
                collection_id,
            )
            return self._api._delete(
                collection_id=_uuid(collection_id),
                ids=delete.ids,
                where=delete.where,
                where_document=delete.where_document,
            )

        return cast(
            List[UUID],
            await to_thread.run_sync(
                process_delete,
                request,
                await request.body(),
                limiter=self._capacity_limiter,
            ),
        )

    @trace_method("FastAPI.count", OpenTelemetryGranularity.OPERATION)
    async def count(
        self,
        request: Request,
        collection_id: str,
    ) -> int:
        self.auth_and_get_tenant_and_database_for_request(
            request.headers,
            AuthzAction.COUNT,
            None,
            None,
            collection_id,
        )

        return cast(
            int,
            await to_thread.run_sync(
                self._api._count,
                _uuid(collection_id),
                limiter=self._capacity_limiter,
            ),
        )

    @trace_method("FastAPI.reset", OpenTelemetryGranularity.OPERATION)
    async def reset(
        self,
        request: Request,
    ) -> bool:
        self.auth_and_get_tenant_and_database_for_request(
            request.headers,
            AuthzAction.RESET,
            None,
            None,
            None,
        )

        return cast(
            bool,
            await to_thread.run_sync(
                self._api.reset,
                limiter=self._capacity_limiter,
            ),
        )

    @trace_method("FastAPI.get_nearest_neighbors", OpenTelemetryGranularity.OPERATION)
    async def get_nearest_neighbors(
        self,
        collection_id: str,
        request: Request,
    ) -> QueryResult:
        def process_query(request: Request, raw_body: bytes) -> QueryResult:
            query = QueryEmbedding.model_validate(orjson.loads(raw_body))

            self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.QUERY,
                None,
                None,
                collection_id,
            )

            return self._api._query(
                collection_id=_uuid(collection_id),
                query_embeddings=query.query_embeddings,
                n_results=query.n_results,
                where=query.where,  # type: ignore
                where_document=query.where_document,  # type: ignore
                include=query.include,
            )

        nnresult = cast(
            QueryResult,
            await to_thread.run_sync(
                process_query,
                request,
                await request.body(),
                limiter=self._capacity_limiter,
            ),
        )
        return nnresult

    async def pre_flight_checks(self) -> Dict[str, Any]:
        def process_pre_flight_checks() -> Dict[str, Any]:
            return {
                "max_batch_size": self._api.get_max_batch_size(),
            }

        return cast(
            Dict[str, Any],
            await to_thread.run_sync(
                process_pre_flight_checks,
                limiter=self._capacity_limiter,
            ),
        )
