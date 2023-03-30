import fastapi
from fastapi import FastAPI as _FastAPI
from fastapi.responses import JSONResponse

from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi import HTTPException, status

import chromadb
import chromadb.server
from chromadb.errors import (
    NoDatapointsException,
    InvalidDimensionException,
    NotEnoughElementsException,
)
from chromadb.server.fastapi.types import (
    AddEmbedding,
    DeleteEmbedding,
    GetEmbedding,
    QueryEmbedding,
    RawSql,  # Results,
    CreateCollection,
    UpdateCollection,
    UpdateEmbedding,
)
from starlette.requests import Request

import logging
from chromadb.telemetry import ServerContext, Telemetry

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


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        logger.exception(e)
        return JSONResponse(content={"error": repr(e)}, status_code=500)


class FastAPI(chromadb.server.Server):
    def __init__(self, settings):
        super().__init__(settings)
        Telemetry.SERVER_CONTEXT = ServerContext.FASTAPI
        self._app = fastapi.FastAPI(debug=True)
        self._api = chromadb.Client(settings)

        self._app.middleware("http")(catch_exceptions_middleware)
        self._app.add_middleware(
            CORSMiddleware,
            allow_headers=["*"],
            allow_origins=settings.chroma_server_cors_allow_origins,
            allow_methods=["*"],
        )

        self.router = fastapi.APIRouter()

        self.router.add_api_route("/api/v1", self.root, methods=["GET"])
        self.router.add_api_route("/api/v1/reset", self.reset, methods=["POST"])
        self.router.add_api_route("/api/v1/version", self.version, methods=["GET"])
        self.router.add_api_route("/api/v1/persist", self.persist, methods=["POST"])
        self.router.add_api_route("/api/v1/raw_sql", self.raw_sql, methods=["POST"])

        self.router.add_api_route("/api/v1/collections", self.list_collections, methods=["GET"])
        self.router.add_api_route("/api/v1/collections", self.create_collection, methods=["POST"])

        self.router.add_api_route(
            "/api/v1/collections/{collection_name}/add",
            self.add,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}/update", self.update, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}/get", self.get, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}/delete", self.delete, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}/count", self.count, methods=["GET"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}/query",
            self.get_nearest_neighbors,
            methods=["POST"],
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}/create_index",
            self.create_index,
            methods=["POST"],
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}", self.get_collection, methods=["GET"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}", self.update_collection, methods=["PUT"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}", self.delete_collection, methods=["DELETE"]
        )

        self._app.include_router(self.router)

        use_route_names_as_operation_ids(self._app)

    def app(self):
        return self._app

    def root(self):
        return {"nanosecond heartbeat": self._api.heartbeat()}

    def persist(self):
        self._api.persist()

    def version(self):
        return self._api.get_version()

    def list_collections(self):
        return self._api.list_collections()

    def create_collection(self, collection: CreateCollection):
        return self._api.create_collection(
            name=collection.name,
            metadata=collection.metadata,
            get_or_create=collection.get_or_create,
        )

    def get_collection(self, collection_name: str):
        return self._api.get_collection(collection_name)

    def update_collection(self, collection_name, collection: UpdateCollection):
        return self._api._modify(
            current_name=collection_name,
            new_name=collection.new_name,
            new_metadata=collection.new_metadata,
        )

    def delete_collection(self, collection_name: str):
        return self._api.delete_collection(collection_name)

    def add(self, collection_name: str, add: AddEmbedding):
        try:
            result = self._api._add(
                collection_name=collection_name,
                embeddings=add.embeddings,
                metadatas=add.metadatas,
                documents=add.documents,
                ids=add.ids,
                increment_index=add.increment_index,
            )
        except InvalidDimensionException as e:
            raise HTTPException(status_code=500, detail=str(e))
        return result

    def update(self, collection_name: str, add: UpdateEmbedding):
        return self._api._update(
            ids=add.ids,
            collection_name=collection_name,
            embeddings=add.embeddings,
            documents=add.documents,
            metadatas=add.metadatas,
        )

    def get(self, collection_name, get: GetEmbedding):
        return self._api._get(
            collection_name=collection_name,
            ids=get.ids,
            where=get.where,
            where_document=get.where_document,
            sort=get.sort,
            limit=get.limit,
            offset=get.offset,
            include=get.include,
        )

    def delete(self, collection_name: str, delete: DeleteEmbedding):
        return self._api._delete(
            where=delete.where,
            ids=delete.ids,
            collection_name=collection_name,
            where_document=delete.where_document,
        )

    def count(self, collection_name: str):
        return self._api._count(collection_name)

    def reset(self):
        return self._api.reset()

    def get_nearest_neighbors(self, collection_name, query: QueryEmbedding):
        try:
            nnresult = self._api._query(
                collection_name=collection_name,
                where=query.where,
                where_document=query.where_document,
                query_embeddings=query.query_embeddings,
                n_results=query.n_results,
                include=query.include,
            )
            return nnresult
        except NoDatapointsException as e:
            raise HTTPException(status_code=500, detail=str(e))
        except InvalidDimensionException as e:
            raise HTTPException(status_code=500, detail=str(e))
        except NotEnoughElementsException as e:
            raise HTTPException(status_code=500, detail=str(e))

    def raw_sql(self, raw_sql: RawSql):
        return self._api.raw_sql(raw_sql.raw_sql)

    def create_index(self, collection_name: str):
        return self._api.create_index(collection_name)
