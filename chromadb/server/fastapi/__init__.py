import fastapi
from fastapi.responses import JSONResponse
from fastapi import status
import chromadb
import chromadb.server
from chromadb.errors import NoDatapointsException
from chromadb.server.fastapi.types import (AddEmbedding, CountEmbedding, DeleteEmbedding,
                                         FetchEmbedding, ProcessEmbedding,
                                         QueryEmbedding, RawSql, #Results,
                                         SpaceKeyInput, CreateCollection, UpdateCollection)

class FastAPI(chromadb.server.Server):

    def __init__(self, settings):
        super().__init__(settings)
        self._app = fastapi.FastAPI(debug=True)
        self._api = chromadb.Client(settings)

        self.router = fastapi.APIRouter()
        self.router.add_api_route("/api/v1", self.root, methods=["GET"])
        self.router.add_api_route("/api/v1/reset", self.reset, methods=["POST"])
        self.router.add_api_route("/api/v1/raw_sql", self.raw_sql, methods=["POST"])

        self.router.add_api_route("/api/v1/collections", self.list_collections, methods=["GET"])
        self.router.add_api_route("/api/v1/collections", self.create_collection, methods=["POST"])

        self.router.add_api_route("/api/v1/collections/{name}/add", self.add, methods=["POST"], status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/api/v1/collections/{name}/update", self.update, methods=["POST"])
        self.router.add_api_route("/api/v1/collections/{name}/fetch", self.fetch, methods=["POST"])
        self.router.add_api_route("/api/v1/collections/{name}/delete", self.delete, methods=["POST"])
        self.router.add_api_route("/api/v1/collections/{name}/count", self.count, methods=["GET"])
        self.router.add_api_route("/api/v1/collections/{name}/search", self.get_nearest_neighbors, methods=["POST"])
        self.router.add_api_route("/api/v1/collections/{name}/create_index", self.create_index, methods=["POST"])
        self.router.add_api_route("/api/v1/collections/{name}", self.get_collection, methods=["GET"])
        self.router.add_api_route("/api/v1/collections/{name}", self.update_collection, methods=["PUT"])
        self.router.add_api_route("/api/v1/collections/{name}", self.delete_collection, methods=["DELETE"])

        self._app.include_router(self.router)


    def app(self):
        return self._app


    def root(self):
        return {"nanosecond heartbeat": self._api.heartbeat()}


    def list_collections(self):
        return self._api.list_collections()

    def create_collection(self, collection: CreateCollection):
        return self._api.create_collection(name=collection.name,metadata=collection.metadata)

    def get_collection(self, name: str):
        return self._api.get_collection(name)
    
    def update_collection(self, name, collection: UpdateCollection):
        return self._api.update_collection(name=name,metadata=collection.metadata)

    def delete_collection(self, name: str):
        return self._api.delete_collection(name)


    def add(self, name: str, add: AddEmbedding):
        return self._api.add(collection_name=name,
                             embeddings=add.embedding,
                             metadatas=add.metadata,
                             documents=add.documents,
                             ids=add.ids
                            )

    def update(self, name: str, add: AddEmbedding):
        return self._api.update(collection_name=name,
                             embedding=add.embedding,
                             metadata=add.metadata
                            )


    def fetch(self, name, fetch: FetchEmbedding):
        # name is passed in the where clause
        df = self._api.fetch(collection_name=name,
                             ids=fetch.ids,
                             where=fetch.where,
                             sort=fetch.sort,
                             limit=fetch.limit,
                             offset=fetch.offset)
        # Would use DataFrame.to_json, but Clickhouse apparently
        # returns some weird bytes that DataFrame.to_json can't
        # handle.

        # Perf was always going to be bad with JSON+dataframe, this
        # shouldn't be too much worse.
        return df.to_dict()


    def delete(self, delete: DeleteEmbedding):
        return self._api.delete(where=delete.where)


    def count(self, name: str):
        return self._api.count(name)


    def reset(self):
        return self._api.reset()


    def get_nearest_neighbors(self, name, query: QueryEmbedding):
        try:
            nnresult = self._api.search(where=query.where,
                                                       embedding=query.embedding,
                                                       n_results=query.n_results)
            nnresult['embeddings'] = nnresult['embeddings'].to_dict()
            return nnresult
        except NoDatapointsException:
            return {"error": "no data points"}


    def raw_sql(self, raw_sql: RawSql):
        return self._api.raw_sql(raw_sql.raw_sql).to_dict()


    def create_index(self, name: str):
        return self._api.create_index(name)

