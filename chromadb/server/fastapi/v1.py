from typing import (
    cast,
    Sequence,
    Optional,
    Tuple,
)
import orjson
from anyio import (
    to_thread,
)
from fastapi import Request, Body
from fastapi import HTTPException, status
from fastapi.routing import APIRoute
from fastapi import FastAPI as _FastAPI

from chromadb.api.configuration import CollectionConfigurationInternal
from chromadb.api.types import (
    Embedding,
    GetResult,
    QueryResult,
    Embeddings,
    convert_list_embeddings_to_np,
)
from chromadb.auth import (
    AuthzAction,
    AuthzResource,
)
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings
from chromadb.errors import (
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
from starlette.datastructures import Headers
from chromadb.server.fastapi import FastAPI, validate_model
from chromadb.utils.fastapi import string_to_uuid as _uuid
from chromadb.types import Database, Tenant
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    add_attributes_to_current_span,
    trace_method,
)
from chromadb.types import Collection as CollectionModel
from chromadb.telemetry.opentelemetry.fastapi import instrument_fastapi
from chromadb.telemetry.product import ProductTelemetryClient
from chromadb.telemetry.product.events import ServerStartEvent


def use_route_names_as_operation_ids(app: _FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.
    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


class FastAPIWithV1(FastAPI):
    def __init__(self, settings: Settings):
        super().__init__(settings)

        self.setup_v1_routes()

        self._app.include_router(self.router)

        use_route_names_as_operation_ids(self._app)
        instrument_fastapi(self._app)
        telemetry_client = self._system.instance(ProductTelemetryClient)
        telemetry_client.capture(ServerStartEvent())

    def setup_v1_routes(self) -> None:
        # =====================================================================
        # OLD ROUTES FOR BACKWARDS COMPATIBILITY — WILL BE REMOVED
        # =====================================================================

        self.router.add_api_route("/api/v1", self.root, methods=["GET"])
        self.router.add_api_route("/api/v1/reset", self.reset, methods=["POST"])
        self.router.add_api_route("/api/v1/version", self.version, methods=["GET"])
        self.router.add_api_route("/api/v1/heartbeat", self.heartbeat, methods=["GET"])
        self.router.add_api_route(
            "/api/v1/pre-flight-checks", self.pre_flight_checks, methods=["GET"]
        )

        self.router.add_api_route(
            "/api/v1/databases",
            self.create_database_v1,
            methods=["POST"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/databases/{database}",
            self.get_database_v1,
            methods=["GET"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/tenants",
            self.create_tenant_v1,
            methods=["POST"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/tenants/{tenant}",
            self.get_tenant_v1,
            methods=["GET"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/collections",
            self.list_collections_v1,
            methods=["GET"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/count_collections",
            self.count_collections_v1,
            methods=["GET"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections",
            self.create_collection_v1,
            methods=["POST"],
            response_model=None,
        )

        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/add",
            self.add_v1,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/update",
            self.update_v1,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/upsert",
            self.upsert_v1,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/get",
            self.get_v1,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/delete",
            self.delete_v1,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/count",
            self.count_v1,
            methods=["GET"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/query",
            self.get_nearest_neighbors_v1,
            methods=["POST"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}",
            self.get_collection_v1,
            methods=["GET"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}",
            self.update_collection_v1,
            methods=["PUT"],
            response_model=None,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}",
            self.delete_collection_v1,
            methods=["DELETE"],
            response_model=None,
        )

        # =====================================================================

    # =========================================================================
    # OLD V1 FUNCTIONS FOR BACKWARD COMPATIBILITY — WILL BE REMOVED
    # =========================================================================

    @trace_method(
        "auth_and_get_tenant_and_database_for_request_v1",
        OpenTelemetryGranularity.OPERATION,
    )
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
            add_attributes_to_current_span(
                {
                    "tenant": tenant,
                    "database": database,
                    "collection": collection,
                }
            )
            return (tenant, database)

        user_identity = self.authn_provider.authenticate_or_raise(dict(headers))

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
        add_attributes_to_current_span(
            {
                "tenant": tenant,
                "database": database,
                "collection": collection,
            }
        )
        return (tenant, database)

    @trace_method("FastAPI.create_database_v1", OpenTelemetryGranularity.OPERATION)
    async def create_database_v1(
        self,
        request: Request,
        tenant: str = DEFAULT_TENANT,
        body: CreateDatabase = Body(...),
    ) -> None:
        def process_create_database(
            tenant: str, headers: Headers, raw_body: bytes
        ) -> None:
            db = validate_model(CreateDatabase, orjson.loads(raw_body))

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

    @trace_method("FastAPI.get_database_v1", OpenTelemetryGranularity.OPERATION)
    async def get_database_v1(
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

    @trace_method("FastAPI.create_tenant_v1", OpenTelemetryGranularity.OPERATION)
    async def create_tenant_v1(
        self, request: Request, body: CreateTenant = Body(...)
    ) -> None:
        def process_create_tenant(request: Request, raw_body: bytes) -> None:
            tenant = validate_model(CreateTenant, orjson.loads(raw_body))

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

    @trace_method("FastAPI.get_tenant_v1", OpenTelemetryGranularity.OPERATION)
    async def get_tenant_v1(
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

    @trace_method("FastAPI.list_collections_v1", OpenTelemetryGranularity.OPERATION)
    async def list_collections_v1(
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

    @trace_method("FastAPI.count_collections_v1", OpenTelemetryGranularity.OPERATION)
    async def count_collections_v1(
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

    @trace_method("FastAPI.create_collection_v1", OpenTelemetryGranularity.OPERATION)
    async def create_collection_v1(
        self,
        request: Request,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
        body: CreateCollection = Body(...),
    ) -> CollectionModel:
        def process_create_collection(
            request: Request, tenant: str, database: str, raw_body: bytes
        ) -> CollectionModel:
            create = validate_model(CreateCollection, orjson.loads(raw_body))
            configuration = (
                CollectionConfigurationInternal()
                if not create.configuration
                else CollectionConfigurationInternal.from_json(create.configuration)
            )

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
                configuration=configuration,
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

    @trace_method("FastAPI.get_collection_v1", OpenTelemetryGranularity.OPERATION)
    async def get_collection_v1(
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

    @trace_method("FastAPI.update_collection_v1", OpenTelemetryGranularity.OPERATION)
    async def update_collection_v1(
        self, collection_id: str, request: Request, body: UpdateCollection = Body(...)
    ) -> None:
        def process_update_collection(
            request: Request, collection_id: str, raw_body: bytes
        ) -> None:
            update = validate_model(UpdateCollection, orjson.loads(raw_body))
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

    @trace_method("FastAPI.delete_collection_v1", OpenTelemetryGranularity.OPERATION)
    async def delete_collection_v1(
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

    @trace_method("FastAPI.add_v1", OpenTelemetryGranularity.OPERATION)
    async def add_v1(
        self, request: Request, collection_id: str, body: AddEmbedding = Body(...)
    ) -> bool:
        try:

            def process_add(request: Request, raw_body: bytes) -> bool:
                add = validate_model(AddEmbedding, orjson.loads(raw_body))
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
                    embeddings=cast(
                        Embeddings,
                        convert_list_embeddings_to_np(add.embeddings)
                        if add.embeddings
                        else None,
                    ),
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

    @trace_method("FastAPI.update_v1", OpenTelemetryGranularity.OPERATION)
    async def update_v1(
        self, request: Request, collection_id: str, body: UpdateEmbedding = Body(...)
    ) -> None:
        def process_update(request: Request, raw_body: bytes) -> bool:
            update = validate_model(UpdateEmbedding, orjson.loads(raw_body))

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
                embeddings=convert_list_embeddings_to_np(update.embeddings)
                if update.embeddings
                else None,
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

    @trace_method("FastAPI.upsert_v1", OpenTelemetryGranularity.OPERATION)
    async def upsert_v1(
        self, request: Request, collection_id: str, body: AddEmbedding = Body(...)
    ) -> None:
        def process_upsert(request: Request, raw_body: bytes) -> bool:
            upsert = validate_model(AddEmbedding, orjson.loads(raw_body))

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
                embeddings=cast(
                    Embeddings,
                    convert_list_embeddings_to_np(upsert.embeddings)
                    if upsert.embeddings
                    else None,
                ),
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

    @trace_method("FastAPI.get_v1", OpenTelemetryGranularity.OPERATION)
    async def get_v1(
        self, collection_id: str, request: Request, body: GetEmbedding = Body(...)
    ) -> GetResult:
        def process_get(request: Request, raw_body: bytes) -> GetResult:
            get = validate_model(GetEmbedding, orjson.loads(raw_body))
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

        get_result = cast(
            GetResult,
            await to_thread.run_sync(
                process_get,
                request,
                await request.body(),
                limiter=self._capacity_limiter,
            ),
        )

        if get_result["embeddings"] is not None:
            get_result["embeddings"] = [
                cast(Embedding, embedding).tolist()
                for embedding in get_result["embeddings"]
            ]

        return get_result

    @trace_method("FastAPI.delete_v1", OpenTelemetryGranularity.OPERATION)
    async def delete_v1(
        self, collection_id: str, request: Request, body: DeleteEmbedding = Body(...)
    ) -> None:
        def process_delete(request: Request, raw_body: bytes) -> None:
            delete = validate_model(DeleteEmbedding, orjson.loads(raw_body))
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

        await to_thread.run_sync(
            process_delete,
            request,
            await request.body(),
            limiter=self._capacity_limiter,
        )

    @trace_method("FastAPI.count_v1", OpenTelemetryGranularity.OPERATION)
    async def count_v1(
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

    @trace_method("FastAPI.reset_v1", OpenTelemetryGranularity.OPERATION)
    async def reset_v1(
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

    @trace_method(
        "FastAPI.get_nearest_neighbors_v1", OpenTelemetryGranularity.OPERATION
    )
    async def get_nearest_neighbors_v1(
        self,
        collection_id: str,
        request: Request,
        body: QueryEmbedding = Body(...),
    ) -> QueryResult:
        def process_query(request: Request, raw_body: bytes) -> QueryResult:
            query = validate_model(QueryEmbedding, orjson.loads(raw_body))

            self.auth_and_get_tenant_and_database_for_request(
                request.headers,
                AuthzAction.QUERY,
                None,
                None,
                collection_id,
            )

            return self._api._query(
                collection_id=_uuid(collection_id),
                query_embeddings=cast(
                    Embeddings,
                    convert_list_embeddings_to_np(query.query_embeddings)
                    if query.query_embeddings
                    else None,
                ),
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

        if nnresult["embeddings"] is not None:
            nnresult["embeddings"] = [
                [cast(Embedding, embedding).tolist() for embedding in result]
                for result in nnresult["embeddings"]
            ]

        return nnresult

    # =========================================================================
