import fastapi
from fastapi.responses import JSONResponse
import chroma
import chroma.server
from chroma.server.fastapi.types import (AddEmbedding, CountEmbedding, DeleteEmbedding,
                                         FetchEmbedding, ProcessEmbedding,
                                         QueryEmbedding, RawSql, Results,
                                         SpaceKeyInput)

class FastAPI(chroma.server.Server):

    def __init__(self):
        super().__init__()
        self._app = fastapi.FastAPI(debug=True)
        self._api = chroma.get_api()

        self.router = fastpi.APIRouter()
        self.router.add_api_route("/api/v1", self.root, methods=["GET"])
        self.router.add_api_route("/api/v1/add", self.add, methods=["POST"], status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/api/v1/fetch", self.fetch, methods=["POST"])
        self.router.add_api_route("/api/v1/delete", self.delete, methods=["POST"])
        self.router.add_api_route("/api/v1/count", self.count, methods=["GET"])
        self.router.add_api_route("/api/v1/reset", self.reset, methods=["POST"])
        self.router.add_api_route("/api/v1/raw_sql", self.raw_sql, methods=["POST"])
        self.router.add_api_route("/api/v1/get_nearest_neighbors", self.get_nearest_neighbors, methods=["POST"])
        self.router.add_api_route("/api/v1/create_index", self.create_index, methods=["POST"])
        self.router.add_api_route("/api/v1/process", self.process, methods=["POST"])
        self.router.add_api_route("/api/v1/get_status", self.get_status, methods=["POST"])
        self.router.add_api_route("/api/v1/get_results", self.get_results, methods=["POST"])

        self._app.include_router(router)


    def root(self):
        return {"nanosecond heartbeat": self._api.heartbeat()}


    def add(self, new_embedding: AddEmbedding):
        if self._api.add(**new_embedding):
            return {"response": "Added records to database"}
        else:
             raise Exception("api.add returned false")


    def fetch(self, embedding: FetchEmbedding):
        return self._api.fetch(embedding)


    def delete(self, embedding: DeleteEmbedding):
        return self._api.delete(embedding)


    def count(self, model_space: str = None):
        return self._api.count(model_space)


    def reset(self):
        return self._api.reset()


    def get_nearest_neighbors(self, embedding: QueryEmbedding):
        return self._api.get_nearest_neighbors(**embedding)


    def raw_sql(self, raw_sql: RawSql):
        return self._api.raw_sql(raw_sql.raw_sql)


    def create_index(self, process_embedding: ProcessEmbedding):
        return self._api.create_index(process_embedding.model_space)


    def process(self, process_embedding: ProcessEmbedding):
        task_id = self._api.process(process_embedding.model_space)
        return JSONResponse({"task_id": task_id})


    def get_status(self, task_id):
        return JSONResponse(self._api.get_task_status(task_id))


     def get_results(self, results: Results):
        return self._api.get_results(results.model_space, results.n_results)
