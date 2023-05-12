from typing import Any, Callable, Dict, List, Sequence
import fastapi
from fastapi import FastAPI as _FastAPI, Response
from fastapi.responses import JSONResponse

from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi import HTTPException, status
from uuid import UUID

import pandas as pd

import chromadb
from chromadb.api.models.Collection import Collection
from chromadb.api.types import GetResult, QueryResult
from chromadb.config import Settings
import chromadb.server
import chromadb.api
from chromadb.errors import (
    ChromaError,
    InvalidUUIDError,
    InvalidDimensionException,
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


class FastAPI(chromadb.server.Server):
    def __init__(self, settings: Settings):
        super().__init__(settings)
        Telemetry.SERVER_CONTEXT = ServerContext.FASTAPI
        self._app = fastapi.FastAPI(debug=True)
        self._api: chromadb.api.API = chromadb.Client(settings)

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
        self.router.add_api_route("/api/v1/heartbeat", self.heartbeat, methods=["GET"])
        self.router.add_api_route("/api/v1/persist", self.persist, methods=["POST"])
        self.router.add_api_route("/api/v1/raw_sql", self.raw_sql, methods=["POST"])

        self.router.add_api_route(
            "/api/v1/collections", self.list_collections, methods=["GET"]
        )
        self.router.add_api_route(
            "/api/v1/collections", self.create_collection, methods=["POST"]
        )

        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/add",
            self.add,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/update", self.update, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/upsert", self.upsert, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/get", self.get, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/delete", self.delete, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/count", self.count, methods=["GET"]
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}/query",
            self.get_nearest_neighbors,
            methods=["POST"],
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}/create_index",
            self.create_index,
            methods=["POST"],
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}",
            self.get_collection,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_id}",
            self.update_collection,
            methods=["PUT"],
        )
        self.router.add_api_route(
            "/api/v1/collections/{collection_name}",
            self.delete_collection,
            methods=["DELETE"],
        )

        self._app.include_router(self.router)

        use_route_names_as_operation_ids(self._app)

    def app(self) -> fastapi.FastAPI:
        return self._app

    def root(self) -> Dict[str, int]:
        return {"nanosecond heartbeat": self._api.heartbeat()}

    def heartbeat(self) -> Dict[str, int]:
        return self.root()

    def persist(self) -> None:
        self._api.persist()

    def version(self) -> str:
        return self._api.get_version()

    def list_collections(self) -> Sequence[Collection]:
        return self._api.list_collections()

    def create_collection(self, collection: CreateCollection) -> Collection:
        return self._api.create_collection(
            name=collection.name,
            metadata=collection.metadata,
            get_or_create=collection.get_or_create,
        )

    def get_collection(self, collection_name: str) -> Collection:
        return self._api.get_collection(collection_name)

    def update_collection(
        self, collection_id: str, collection: UpdateCollection
    ) -> None:
        return self._api._modify(
            id=_uuid(collection_id),
            new_name=collection.new_name,
            new_metadata=collection.new_metadata,
        )

    def delete_collection(self, collection_name: str) -> None:
        return self._api.delete_collection(collection_name)

    def add(self, collection_id: str, add: AddEmbedding) -> None:
        try:
            result = self._api._add(
                collection_id=_uuid(collection_id),
                embeddings=add.embeddings,
                metadatas=add.metadatas,
                documents=add.documents,
                ids=add.ids,
                increment_index=add.increment_index,
            )
        except InvalidDimensionException as e:
            raise HTTPException(status_code=500, detail=str(e))
        return result

    def update(self, collection_id: str, add: UpdateEmbedding) -> None:
        return self._api._update(
            ids=add.ids,
            collection_id=_uuid(collection_id),
            embeddings=add.embeddings,
            documents=add.documents,
            metadatas=add.metadatas,
        )

    def upsert(self, collection_id: str, upsert: AddEmbedding) -> None:
        return self._api._upsert(
            collection_id=_uuid(collection_id),
            ids=upsert.ids,
            embeddings=upsert.embeddings,
            documents=upsert.documents,
            metadatas=upsert.metadatas,
            increment_index=upsert.increment_index,
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

    def delete(self, collection_id: str, delete: DeleteEmbedding) -> List[UUID]:
        return self._api._delete(
            where=delete.where,
            ids=delete.ids,
            collection_id=_uuid(collection_id),
            where_document=delete.where_document,
        )

    def count(self, collection_id: str) -> int:
        return self._api._count(_uuid(collection_id))

    def reset(self) -> bool:
        return self._api.reset()

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

    def raw_sql(self, raw_sql: RawSql) -> pd.DataFrame:
        return self._api.raw_sql(raw_sql.raw_sql)

    def create_index(self, collection_name: str) -> bool:
        return self._api.create_index(collection_name)
