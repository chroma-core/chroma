import fastapi
from fastapi.responses import JSONResponse
from fastapi import status
import chroma
import chroma.server
from chroma.errors import NoDatapointsException
from chroma.server.fastapi.types import (AddEmbedding, CountEmbedding, DeleteEmbedding,
                                         FetchEmbedding, ProcessEmbedding,
                                         QueryEmbedding, RawSql, #Results,
                                         SpaceKeyInput)

class FastAPI(chroma.server.Server):

    def __init__(self, settings):
        super().__init__(settings)
        self._app = fastapi.FastAPI(debug=True)
        self._api = chroma.get_api(settings)

        self.router = fastapi.APIRouter()
        self.router.add_api_route("/api/v1", self.root, methods=["GET"])
        self.router.add_api_route("/api/v1/add", self.add, methods=["POST"], status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/api/v1/fetch", self.fetch, methods=["POST"])
        self.router.add_api_route("/api/v1/delete", self.delete, methods=["POST"])
        self.router.add_api_route("/api/v1/count", self.count, methods=["GET"])
        self.router.add_api_route("/api/v1/reset", self.reset, methods=["POST"])
        self.router.add_api_route("/api/v1/raw_sql", self.raw_sql, methods=["POST"])
        self.router.add_api_route("/api/v1/get_nearest_neighbors", self.get_nearest_neighbors, methods=["POST"])
        self.router.add_api_route("/api/v1/create_index", self.create_index, methods=["POST"])

        self._app.include_router(self.router)


    def app(self):
        return self._app


    def root(self):
        return {"nanosecond heartbeat": self._api.heartbeat()}


    def add(self, add: AddEmbedding):
        return self._api.add(model_space=add.model_space,
                             embedding=add.embedding,
                             input_uri=add.input_uri,
                             dataset=add.dataset,
                             metadata=add.metadata
                             )


    def fetch(self, fetch: FetchEmbedding):
        df = self._api.fetch(where=fetch.where,
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


    def count(self, model_space: str = None):
        return self._api.count(model_space)


    def reset(self):
        return self._api.reset()


    def get_nearest_neighbors(self, query: QueryEmbedding):
        try:
            nnresult = self._api.get_nearest_neighbors(where=query.where,
                                                       embedding=query.embedding,
                                                       n_results=query.n_results)
            nnresult['embeddings'] = nnresult['embeddings'].to_dict()
            return nnresult
        except NoDatapointsException:
            return {"error": "no data points"}


    def raw_sql(self, raw_sql: RawSql):
        return self._api.raw_sql(raw_sql.raw_sql).to_dict()


    def create_index(self, process: ProcessEmbedding):
        return self._api.create_index(process.model_space)

