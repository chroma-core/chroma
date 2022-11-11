import os
import shutil
import time

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from chroma_server.worker import heavy_offline_analysis

from chroma_server.db.clickhouse import Clickhouse, get_col_pos
from chroma_server.index.hnswlib import Hnswlib
from chroma_server.types import AddEmbedding, QueryEmbedding, ProcessEmbedding, FetchEmbedding, CountEmbedding, RawSql, Results, SpaceKeyInput

from chroma_server.utils.telemetry.capture import Capture
from chroma_server.utils.error_reporting import init_error_reporting

chroma_telemetry = Capture()
chroma_telemetry.capture('server-start')
init_error_reporting()

from celery.result import AsyncResult

# Boot script
db = Clickhouse
ann_index = Hnswlib

app = FastAPI(debug=True)

# init db and index
app._db = db()
app._ann_index = ann_index()

# API Endpoints
@app.get("/api/v1")
async def root():
    '''Heartbeat endpoint'''
    return {"nanosecond heartbeat": int(1000 * time.time_ns())}


@app.get("/api/v1/calculate_results")
async def calculate_results(space_key: SpaceKeyInput):
    task = heavy_offline_analysis.delay(space_key.space_key)
    return JSONResponse({"task_id": task.id})

@app.get("/api/v1/tasks/{task_id}")
async def get_status(task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JSONResponse(result)

@app.get("/api/v1/get_results")
async def get_results(results: Results):
    return app._db.return_results(results.space_key, results.n_results)

    

@app.post("/api/v1/add", status_code=status.HTTP_201_CREATED)
async def add_to_db(new_embedding: AddEmbedding):
    '''Save batched embeddings to database'''
    app._db.add_batch(
        new_embedding.space_key, 
        new_embedding.embedding_data, 
        new_embedding.input_uri, 
        new_embedding.dataset,
        new_embedding.custom_quality_score, 
        new_embedding.category_name
    )

    return {"response": "Added records to database"}


@app.get("/api/v1/process")
async def process(process_embedding: ProcessEmbedding):
    '''
    Currently generates an index for the embedding db
    '''
    fetch = app._db.fetch({"space_key": process_embedding.space_key}, columnar=True)
    app._ann_index.run(process_embedding.space_key, fetch[1], fetch[2]) # more magic number, ugh


@app.post("/api/v1/fetch")
async def fetch(embedding: FetchEmbedding):
    '''
    Fetches embeddings from the database
    - enables filtering by where_filter, sorting by key, and limiting the number of results
    '''
    return app._db.fetch(embedding.where_filter, embedding.sort, embedding.limit)


@app.get("/api/v1/count")
async def count(count_embedding: CountEmbedding):
    '''
    Returns the number of records in the database
    '''
    return {"count": app._db.count(space_key=count_embedding.space_key)}


@app.get("/api/v1/reset")
async def reset():
    '''
    Reset the database and index - WARNING: Destructive! 
    '''
    app._db = db()
    app._db.reset()
    app._ann_index = ann_index()
    app._ann_index.reset()
    return True

@app.post("/api/v1/get_nearest_neighbors")
async def get_nearest_neighbors(embedding: QueryEmbedding):
    '''
    return the distance, database ids, and embedding themselves for the input embedding
    '''
    if embedding.space_key is None:
        return {"error": "space_key is required"}

    ids = None
    filter_by_where = {}
    filter_by_where["space_key"] = embedding.space_key
    if embedding.category_name is not None:
        filter_by_where["category_name"] = embedding.category_name
    if embedding.dataset is not None:
        filter_by_where["dataset"] = embedding.dataset

    if filter_by_where is not None:
        results = app._db.fetch(filter_by_where)
        ids = [str(item[get_col_pos('uuid')]) for item in results] 
    
    uuids, distances = app._ann_index.get_nearest_neighbors(embedding.space_key, embedding.embedding, embedding.n_results, ids)
    return {
        "ids": uuids,
        "embeddings": app._db.get_by_ids(uuids),
        "distances": distances.tolist()[0]
    }

@app.get("/api/v1/raw_sql")
async def raw_sql(raw_sql: RawSql):
    return app._db.raw_sql(raw_sql.raw_sql)
