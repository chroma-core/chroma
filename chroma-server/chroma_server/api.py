import time
import os

from chroma_server.utils.sampling import score_and_store, get_sample

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
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

chroma_telemetry = Capture()
chroma_telemetry.capture('server-start')
init_error_reporting()

from celery.result import AsyncResult

# current valid modes are 'in-memory' and 'docker', it defaults to docker
chroma_mode = os.getenv('CHROMA_MODE', 'docker')
if chroma_mode == 'in-memory':
    db = DuckDB
else:
    db = Clickhouse

ann_index = Hnswlib

app = FastAPI(debug=True)

# init db and index
app._db = db()
app._ann_index = ann_index()

def create_index_data_dir():
    if not os.path.exists(os.getcwd() + '/index_data'):
        os.makedirs(os.getcwd() + '/index_data')
    app._ann_index.set_save_folder(os.getcwd() + '/index_data')

if chroma_mode == 'in-memory':
    create_index_data_dir()

# API Endpoints
@app.get("/api/v1")
async def root():
    '''Heartbeat endpoint'''
    return {"nanosecond heartbeat": int(1000 * time.time_ns())}
    

@app.post("/api/v1/add", status_code=status.HTTP_201_CREATED)
async def add(new_embedding: AddEmbedding):
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

    app._db.add(
        model_space, 
        new_embedding.embedding, 
        new_embedding.input_uri, 
        dataset,
        new_embedding.inference_class, 
        new_embedding.label_class
    )

    return {"response": "Added records to database"}


@app.post("/api/v1/fetch")
async def fetch(embedding: FetchEmbedding):
    '''
    Fetches embeddings from the database
    - enables filtering by where, sorting by key, and limiting the number of results
    '''
    return app._db.fetch(embedding.where, embedding.sort, embedding.limit, embedding.offset)

@app.post("/api/v1/delete")
async def delete(embedding: DeleteEmbedding):
    '''
    Deletes embeddings from the database
    - enables filtering by where
    '''
    deleted_uuids = app._db.delete(embedding.where)
    if len(embedding.where) == 1:
        if 'model_space' in embedding.where:
            app._ann_index.delete(embedding.where['model_space'])

    deleted_uuids = [uuid[0] for uuid in deleted_uuids] # de-tuple
    app._ann_index.delete_from_index(embedding.where['model_space'], deleted_uuids)
    return deleted_uuids

@app.get("/api/v1/count")
async def count(model_space: str = None):
    '''
    Returns the number of records in the database
    '''
    return {"count": app._db.count(model_space=model_space)}



@app.post("/api/v1/reset")
async def reset():
    '''
    Reset the database and index - WARNING: Destructive! 
    '''
    app._db = db()
    app._db.reset()
    app._ann_index.reset() # this has to come first I think
    app._ann_index = ann_index()
    if chroma_mode == 'in-memory':
        create_index_data_dir()
    return True

@app.post("/api/v1/get_nearest_neighbors")
async def get_nearest_neighbors(embedding: QueryEmbedding):
    '''
    return the distance, database ids, and embedding themselves for a single input embedding
    '''
    if embedding.where['model_space'] is None:
        return {"error": "model_space is required"}

    results = app._db.fetch(embedding.where)
    ids = []
    if len(results) > 0:
        ids = results.uuid.tolist() 
    else:
        return {"error": "No datapoints found for the supplied filter"}

    uuids, distances = app._ann_index.get_nearest_neighbors(embedding.where['model_space'], embedding.embedding, embedding.n_results, ids)
    return {
        "ids": uuids,
        "embeddings": app._db.get_by_ids(uuids[0]),
        "distances": distances.tolist()[0]
    }

@app.post("/api/v1/raw_sql")
async def raw_sql(raw_sql: RawSql):
    return app._db.raw_sql(raw_sql.raw_sql)

@app.post("/api/v1/create_index")
async def create_index(process_embedding: ProcessEmbedding):
    '''
    Currently generates an index for the embedding db
    '''
    fetch = app._db.fetch({"model_space": process_embedding.model_space})
    chroma_telemetry.capture('created-index-run-process', {'n': len(fetch)})
    app._ann_index.run(process_embedding.model_space, fetch.uuid.tolist(), fetch.embedding.tolist()) # more magic number, ugh

@app.post("/api/v1/process")
async def process(process_embedding: ProcessEmbedding):
    '''
    Currently generates an index for the embedding db
    '''
    if chroma_mode == 'in-memory':
        raise Exception("in-memory mode does not process because it relies on celery and redis")

    fetch = app._db.fetch({"model_space": process_embedding.model_space})
    chroma_telemetry.capture('created-index-run-process', {'n': len(fetch)})
    app._ann_index.run(process_embedding.model_space, fetch.uuid.tolist(), fetch.embedding.tolist()) # more magic number, ugh

    chroma_telemetry.capture('score_and_store')
    score_and_store(
        training_dataset_name=process_embedding.training_dataset_name,
        inference_dataset_name=process_embedding.inference_dataset_name,
        db_connection=app._db,
        ann_index=app._ann_index,
        model_space=process_embedding.model_space,
    )
    return True

@app.post("/api/v1/tasks/{task_id}")
async def get_status(task_id):
    if chroma_mode == 'in-memory':
        raise Exception("in-memory mode does not process because it relies on celery and redis")
        
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JSONResponse(result)

@app.post("/api/v1/get_results")
async def get_results(results: Results):
    if chroma_mode == 'in-memory':
        raise Exception("in-memory mode does not process because it relies on celery and redis")

    # if there is no index, generate one
    if not app._ann_index.has_index(results.model_space):
        raise Exception("no index found for model space: ", results.model_space, " - please run the process endpoint first")

    # if there are no results, generate them
    if app._db.count_results(results.model_space) == 0:
        raise Exception("no results found for model space: ", results.model_space, " - please run the process endpoint first")

    else:
        sample_proportions = {
            "activation_uncertainty": 0.3,
            "boundary_uncertainty": 0.3,
            "representative_cluster_outlier": 0.2,
            "random": 0.2,
        }
        return get_sample(n_samples=1000, sample_proportions=sample_proportions, db_connection=app._db, model_space=results.model_space)

# headless mode
core = app
core.add = add
core.count = count
core.fetch = fetch
core.reset = reset
core.delete = delete
core.get_nearest_neighbors = get_nearest_neighbors
core.raw_sql = raw_sql
core.create_index = create_index

# these as currently constructed require celery
# chroma_core.process = process
# chroma_core.get_status = get_status
# chroma_core.get_results = get_results
