from celery.result import AsyncResult
import time
import os

from chroma_server.db.clickhouse import Clickhouse, get_col_pos
from chroma_server.db.duckdb import DuckDB
from chroma_server.index.hnswlib import Hnswlib
from chroma_server.types import (AddEmbedding, CountEmbedding, DeleteEmbedding,
                                 FetchEmbedding, ProcessEmbedding,
                                 QueryEmbedding, RawSql, Results,
                                 SpaceKeyInput)
from chroma_server.utils.error_reporting import init_error_reporting
from chroma_server.utils.telemetry.capture import Capture
from chroma_server.worker import heavy_offline_analysis
from fastapi import FastAPI, status, APIRouter
from fastapi.responses import JSONResponse

class ChromaRouter():

    _app = None
    _db = None
    _ann_index = None

    def __init__(self, app: FastAPI, db, ann_index: Hnswlib):
        self._app = app
        self._db = db
        self._ann_index = ann_index

        self.router = APIRouter()
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

    # API Endpoints
    # @app.get("/api/v1")
    def root(self):
        '''Heartbeat endpoint'''
        return {"nanosecond heartbeat": int(1000 * time.time_ns())}

    # @app.post("/api/v1/add", status_code=status.HTTP_201_CREATED)
    def add(self, new_embedding: AddEmbedding):
        '''Save batched embeddings to database'''

        number_of_embeddings = len(new_embedding.embedding)

        if isinstance(new_embedding.model_space, str):
            model_space = [new_embedding.model_space] * number_of_embeddings
        elif len(new_embedding.model_space) == 1: 
            model_space = [new_embedding.model_space[0]] * number_of_embeddings
        else: 
            model_space = new_embedding.model_space
        
        if isinstance(new_embedding.dataset, str):
            dataset = [new_embedding.dataset] * number_of_embeddings
        elif len(new_embedding.dataset) == 1:
            dataset = [new_embedding.dataset[0]] * number_of_embeddings
        else: 
            dataset = new_embedding.dataset

        self._app._db.add(
            model_space, 
            new_embedding.embedding, 
            new_embedding.input_uri, 
            dataset,
            new_embedding.inference_class, 
            new_embedding.label_class
        )

        return {"response": "Added records to database"}

    # @app.post("/api/v1/fetch")
    def fetch(self, embedding: FetchEmbedding):
        '''
        Fetches embeddings from the database
        - enables filtering by where, sorting by key, and limiting the number of results
        '''
        return self._app._db.fetch(embedding.where, embedding.sort, embedding.limit, embedding.offset)

    # @app.post("/api/v1/delete")
    def delete(self, embedding: DeleteEmbedding):
        '''
        Deletes embeddings from the database
        - enables filtering by where
        '''
        deleted_uuids = self._app._db.delete(embedding.where)
        if len(embedding.where) == 1:
            if 'model_space' in embedding.where:
                self._app._ann_index.delete(embedding.where['model_space'])

        deleted_uuids = [uuid[0] for uuid in deleted_uuids] # de-tuple
        self._app._ann_index.delete_from_index(embedding.where['model_space'], deleted_uuids)
        return deleted_uuids

    # @app.get("/api/v1/count")
    def count(self, model_space: str = None):
        '''
        Returns the number of records in the database
        '''
        return {"count": self._app._db.count(model_space=model_space)}

    # @app.post("/api/v1/reset")
    def reset(self):
        '''
        Reset the database and index - WARNING: Destructive! 
        '''
        self._app._db = self._db()
        self._app._db.reset()
        self._app._ann_index.reset() # this has to come first I think
        self._app._ann_index = self._ann_index()
        # if chroma_mode == 'in-memory':
        #     create_index_data_dir()
        return True

    # @app.post("/api/v1/get_nearest_neighbors")
    def get_nearest_neighbors(self,embedding: QueryEmbedding):
        '''
        return the distance, database ids, and embedding themselves for the input embedding
        '''
        if embedding.where['model_space'] is None:
            return {"error": "model_space is required"}

        results = self._app._db.fetch(embedding.where)
        ids = [str(item[get_col_pos('uuid')]) for item in results] 

        uuids, distances = self._app._ann_index.get_nearest_neighbors(embedding.where['model_space'], embedding.embedding, embedding.n_results, ids)
        return {
            "ids": uuids,
            "embeddings": self._app._db.get_by_ids(uuids),
            "distances": distances.tolist()[0]
        }

    # @app.post("/api/v1/raw_sql")
    def raw_sql(self, raw_sql: RawSql):
        return self._app._db.raw_sql(raw_sql.raw_sql)

    # @app.post("/api/v1/create_index")
    def create_index(self, process_embedding: ProcessEmbedding):
        '''
        Currently generates an index for the embedding db
        '''
        fetch = self._app._db.fetch({"model_space": process_embedding.model_space}, columnar=True)
        # chroma_telemetry.capture('created-index-run-process', {'n': len(fetch[2])})
        self._app._ann_index.run(process_embedding.model_space, fetch[1], fetch[2]) # more magic number, ugh

    # @app.post("/api/v1/process")
    def process(self, process_embedding: ProcessEmbedding):
        '''
        Currently generates an index for the embedding db
        '''
        # if chroma_mode == 'in-memory':
        #     raise Exception("in-memory mode does not process because it relies on celery and redis")

        fetch = self._app._db.fetch({"model_space": process_embedding.model_space}, columnar=True)
        # chroma_telemetry.capture('created-index-run-process', {'n': len(fetch[2])})
        self._app._ann_index.run(process_embedding.model_space, fetch[1], fetch[2]) # more magic number, ugh

        task = heavy_offline_analysis.delay(process_embedding.model_space)
        # chroma_telemetry.capture('heavy-offline-analysis')
        return JSONResponse({"task_id": task.id})

    # @app.post("/api/v1/tasks/{task_id}")
    def get_status(self, task_id):
        # if chroma_mode == 'in-memory':
        #     raise Exception("in-memory mode does not process because it relies on celery and redis")
            
        task_result = AsyncResult(task_id)
        result = {
            "task_id": task_id,
            "task_status": task_result.status,
            "task_result": task_result.result
        }
        return JSONResponse(result)

    # @app.post("/api/v1/get_results")
    def get_results(self, results: Results):
        # if chroma_mode == 'in-memory':
        #     raise Exception("in-memory mode does not process because it relies on celery and redis")

        # if there is no index, generate one
        if not self._app._ann_index.has_index(results.model_space):
            fetch = self._app._db.fetch({"model_space": results.model_space}, columnar=True)
            # chroma_telemetry.capture('run-process', {'n': len(fetch[2])})
            print("Generating index for model space: ", results.model_space, " with ", len(fetch[2]), " embeddings")
            self._app._ann_index.run(results.model_space, fetch[1], fetch[2]) # more magic number, ugh
            print("Done generating index for model space: ", results.model_space)

        # if there are no results, generate them
        print("self._app._db.count_results(results.model_space): ", self._app._db.count_results(results.model_space))
        if self._app._db.count_results(results.model_space) == 0:
            print("starting heavy offline analysis")
            task = heavy_offline_analysis(results.model_space)
            print("ending heavy offline analysis")
            return self._app._db.return_results(results.model_space, results.n_results)

        else:
            return self._app._db.return_results(results.model_space, results.n_results)