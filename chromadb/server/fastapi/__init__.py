from typing import Any, Callable, Dict, List, Sequence
import fastapi
from fastapi import FastAPI as _FastAPI, Response
from fastapi.responses import JSONResponse

from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi import HTTPException, status
from uuid import UUID
import chromadb
from chromadb.api.models.Collection import Collection
from chromadb.api.types import GetResult, QueryResult
from chromadb.auth import (
    AuthzDynamicParams,
    AuthzResourceActions,
    AuthzResourceTypes,
    DynamicAuthzResource,
)
from chromadb.auth.fastapi import (
    FastAPIChromaAuthMiddleware,
    FastAPIChromaAuthMiddlewareWrapper,
    FastAPIChromaAuthzMiddleware,
    FastAPIChromaAuthzMiddlewareWrapper,
    authz_context,
)
from chromadb.auth.fastapi_utils import (
    attr_from_collection_lookup,
    attr_from_resource_object,
)
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
import chromadb.server
import chromadb.api
from chromadb.api import ServerAPI
from chromadb.errors import (
    ChromaError,
    InvalidUUIDError,
    InvalidDimensionException,
)
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
from starlette.requests import Request

import logging
from chromadb.telemetry.opentelemetry.fastapi import instrument_fastapi
from chromadb.types import Database, Tenant
from chromadb.telemetry.product import ServerContext, ProductTelemetryClient
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)

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
        return JSONResponse(
            content={"error": e.name(), "message": e.message()}, status_code=e.code()
        )
    except Exception as e:
        logger.exception(e)
        return JSONResponse(content={"error": repr(e)}, status_code=500)


def _uuid(uuid_str: str) -> UUID:
    try:
        return UUID(uuid_str)
    except ValueError:
        raise InvalidUUIDError(f"Could not parse {uuid_str} as a UUID")


class ChromaAPIRouter(fastapi.APIRouter):  # type: ignore
    # A simple subclass of fastapi's APIRouter which treats URLs with a trailing "/" the
    # same as URLs without. Docs will only contain URLs without trailing "/"s.
    def add_api_route(self, path: str, *args: Any, **kwargs: Any) -> None:
        # If kwargs["include_in_schema"] isn't passed OR is True, we should only
        # include the non-"/" path. If kwargs["include_in_schema"] is False, include
        # neither.
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


class FastAPI(chromadb.server.Server):
    def __init__(self, settings: Settings):
        super().__init__(settings)
        ProductTelemetryClient.SERVER_CONTEXT = ServerContext.FASTAPI
        self._app = fastapi.FastAPI(debug=True)
        self._system = System(settings)
        self._api: ServerAPI = self._system.instance(ServerAPI)
        self._opentelemetry_client = self._api.require(OpenTelemetryClient)
        self._system.start()

        self._app.middleware("http")(catch_exceptions_middleware)
        self._app.add_middleware(
            CORSMiddleware,
            allow_headers=["*"],
            allow_origins=settings.chroma_server_cors_allow_origins,
            allow_methods=["*"],
        )

        if settings.chroma_server_authz_provider:
            self._app.add_middleware(
                FastAPIChromaAuthzMiddlewareWrapper,
                authz_middleware=self._api.require(FastAPIChromaAuthzMiddleware),
            )

        if settings.chroma_server_auth_provider:
            self._app.add_middleware(
                FastAPIChromaAuthMiddlewareWrapper,
                auth_middleware=self._api.require(FastAPIChromaAuthMiddleware),
            )

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

    def app(self) -> fastapi.FastAPI:
        return self._app

    def root(self) -> Dict[str, int]:
        return {"nanosecond heartbeat": self._api.heartbeat()}

    def heartbeat(self) -> Dict[str, int]:
        return self.root()

    def version(self) -> str:
        return self._api.get_version()

    @trace_method("FastAPI.create_database", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.CREATE_DATABASE,
        resource=DynamicAuthzResource(
            type=AuthzResourceTypes.DB,
            attributes=attr_from_resource_object(
                type=AuthzResourceTypes.DB, additional_attrs=["tenant"]
            ),
        ),
    )
    def create_database(
        self, database: CreateDatabase, tenant: str = DEFAULT_TENANT
    ) -> None:
        return self._api.create_database(database.name, tenant)

    @trace_method("FastAPI.get_database", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.GET_DATABASE,
        resource=DynamicAuthzResource(
            id="*",
            type=AuthzResourceTypes.DB,
            attributes=AuthzDynamicParams.dict_from_function_kwargs(
                arg_names=["tenant", "database"]
            ),
        ),
    )
    def get_database(self, database: str, tenant: str = DEFAULT_TENANT) -> Database:
        return self._api.get_database(database, tenant)

    @trace_method("FastAPI.create_tenant", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.CREATE_TENANT,
        resource=DynamicAuthzResource(
            type=AuthzResourceTypes.TENANT,
        ),
    )
    def create_tenant(self, tenant: CreateTenant) -> None:
        return self._api.create_tenant(tenant.name)

    @trace_method("FastAPI.get_tenant", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.GET_TENANT,
        resource=DynamicAuthzResource(
            id="*",
            type=AuthzResourceTypes.TENANT,
        ),
    )
    def get_tenant(self, tenant: str) -> Tenant:
        return self._api.get_tenant(tenant)

    @trace_method("FastAPI.list_collections", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.LIST_COLLECTIONS,
        resource=DynamicAuthzResource(
            id="*",
            type=AuthzResourceTypes.DB,
            attributes=AuthzDynamicParams.dict_from_function_kwargs(
                arg_names=["tenant", "database"]
            ),
        ),
    )
    def list_collections(
        self, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> Sequence[Collection]:
        return self._api.list_collections(tenant=tenant, database=database)

    @trace_method("FastAPI.create_collection", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.CREATE_COLLECTION,
        resource=DynamicAuthzResource(
            id="*",
            type=AuthzResourceTypes.DB,
            attributes=AuthzDynamicParams.dict_from_function_kwargs(
                arg_names=["tenant", "database"]
            ),
        ),
    )
    def create_collection(
        self,
        collection: CreateCollection,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Collection:
        return self._api.create_collection(
            name=collection.name,
            metadata=collection.metadata,
            get_or_create=collection.get_or_create,
            tenant=tenant,
            database=database,
        )

    @trace_method("FastAPI.get_collection", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.GET_COLLECTION,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_name"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=AuthzDynamicParams.dict_from_function_kwargs(
                arg_names=["tenant", "database"]
            ),
        ),
    )
    def get_collection(
        self,
        collection_name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Collection:
        return self._api.get_collection(
            collection_name, tenant=tenant, database=database
        )

    @trace_method("FastAPI.update_collection", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.UPDATE_COLLECTION,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_id"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=attr_from_collection_lookup(collection_id_arg="collection_id"),
        ),
    )
    def update_collection(
        self, collection_id: str, collection: UpdateCollection
    ) -> None:
        return self._api._modify(
            id=_uuid(collection_id),
            new_name=collection.new_name,
            new_metadata=collection.new_metadata,
        )

    @trace_method("FastAPI.delete_collection", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.DELETE_COLLECTION,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_name"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=AuthzDynamicParams.dict_from_function_kwargs(
                arg_names=["tenant", "database"]
            ),
        ),
    )
    def delete_collection(
        self,
        collection_name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        return self._api.delete_collection(
            collection_name, tenant=tenant, database=database
        )

    @trace_method("FastAPI.add", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.ADD,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_id"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=attr_from_collection_lookup(collection_id_arg="collection_id"),
        ),
    )
    def add(self, collection_id: str, add: AddEmbedding) -> None:
        try:
            result = self._api._add(
                collection_id=_uuid(collection_id),
                embeddings=add.embeddings,  # type: ignore
                metadatas=add.metadatas,  # type: ignore
                documents=add.documents,  # type: ignore
                uris=add.uris,  # type: ignore
                ids=add.ids,
            )
        except InvalidDimensionException as e:
            raise HTTPException(status_code=500, detail=str(e))
        return result  # type: ignore

    @trace_method("FastAPI.update", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.UPDATE,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_id"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=attr_from_collection_lookup(collection_id_arg="collection_id"),
        ),
    )
    def update(self, collection_id: str, add: UpdateEmbedding) -> None:
        self._api._update(
            ids=add.ids,
            collection_id=_uuid(collection_id),
            embeddings=add.embeddings,
            documents=add.documents,  # type: ignore
            uris=add.uris,  # type: ignore
            metadatas=add.metadatas,  # type: ignore
        )

    @trace_method("FastAPI.upsert", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.UPSERT,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_id"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=attr_from_collection_lookup(collection_id_arg="collection_id"),
        ),
    )
    def upsert(self, collection_id: str, upsert: AddEmbedding) -> None:
        self._api._upsert(
            collection_id=_uuid(collection_id),
            ids=upsert.ids,
            embeddings=upsert.embeddings,  # type: ignore
            documents=upsert.documents,  # type: ignore
            uris=upsert.uris,  # type: ignore
            metadatas=upsert.metadatas,  # type: ignore
        )

    @trace_method("FastAPI.get", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.GET,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_id"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=attr_from_collection_lookup(collection_id_arg="collection_id"),
        ),
    )
    def get(self, collection_id: str, get: GetEmbedding) -> GetResult:
        return self._api._get(
            collection_id=_uuid(collection_id),
            ids=get.ids,
            where=get.where,
            where_document=get.where_document,
            sort=get.sort,
            limit=get.limit,
            offset=get.offset,
            include=get.include,
        )

    @trace_method("FastAPI.delete", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.DELETE,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_id"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=attr_from_collection_lookup(collection_id_arg="collection_id"),
        ),
    )
    def delete(self, collection_id: str, delete: DeleteEmbedding) -> List[UUID]:
        return self._api._delete(
            where=delete.where,  # type: ignore
            ids=delete.ids,
            collection_id=_uuid(collection_id),
            where_document=delete.where_document,
        )

    @trace_method("FastAPI.count", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.COUNT,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_id"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=attr_from_collection_lookup(collection_id_arg="collection_id"),
        ),
    )
    def count(self, collection_id: str) -> int:
        return self._api._count(_uuid(collection_id))

    @trace_method("FastAPI.reset", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.RESET,
        resource=DynamicAuthzResource(
            id="*",
            type=AuthzResourceTypes.DB,
        ),
    )
    def reset(self) -> bool:
        return self._api.reset()

    @trace_method("FastAPI.get_nearest_neighbors", OpenTelemetryGranularity.OPERATION)
    @authz_context(
        action=AuthzResourceActions.QUERY,
        resource=DynamicAuthzResource(
            id=AuthzDynamicParams.from_function_kwargs(arg_name="collection_id"),
            type=AuthzResourceTypes.COLLECTION,
            attributes=attr_from_collection_lookup(collection_id_arg="collection_id"),
        ),
    )
    def get_nearest_neighbors(
        self, collection_id: str, query: QueryEmbedding
    ) -> QueryResult:
        nnresult = self._api._query(
            collection_id=_uuid(collection_id),
            where=query.where,  # type: ignore
            where_document=query.where_document,  # type: ignore
            query_embeddings=query.query_embeddings,
            n_results=query.n_results,
            include=query.include,
        )
        return nnresult

    def pre_flight_checks(self) -> Dict[str, Any]:
        return {
            "max_batch_size": self._api.max_batch_size,
        }
