import os
import shutil
import time

from fastapi import FastAPI, Response, status

from chroma_server.db.duckdb import DuckDB
from chroma_server.index.hnswlib import Hnswlib
from chroma_server.algorithms.rand_subsample import rand_bisectional_subsample
from chroma_server.types import AddEmbedding, QueryEmbedding
from chroma_server.logger import logger
from chroma_server.utils.telemetry.capture import Capture

# Boot script
db = DuckDB
ann_index = Hnswlib

app = FastAPI(debug=True)

# init db and index
app._db = db()
app._ann_index = ann_index()

if not os.path.exists(".chroma"):
    os.mkdir(".chroma")

if os.path.exists(".chroma/chroma.parquet"):
    logger.info("Loading existing chroma database")
    app._db.load()

if os.path.exists(".chroma/index.bin"):
    logger.info("Loading existing chroma index")
    app._ann_index.load(app._db.count(), len(app._db.fetch(limit=1).embedding_data))

chroma_telemetry = Capture()
chroma_telemetry.capture('server-start')

# API Endpoints
@app.get("/api/v1")
async def root():
    '''
    Heartbeat endpoint
    '''
    return {"nanosecond heartbeat": int(1000 * time.time_ns())}

@app.post("/api/v1/add", status_code=status.HTTP_201_CREATED)
async def add_to_db(new_embedding: AddEmbedding):
    '''
    Save embedding to database
    - supports single or batched embeddings
    '''

    app._db.add_batch(
        new_embedding.embedding_data, 
        new_embedding.input_uri, 
        new_embedding.dataset,
        new_embedding.custom_quality_score, 
        new_embedding.category_name
        )

    return {"response": "Added record to database"}

@app.get("/api/v1/process")
async def process():
    '''
    Currently generates an index for the embedding db
    '''
    app._ann_index.run(app._db.fetch())

@app.get("/api/v1/fetch")
async def fetch(where_filter={}, sort=None, limit=None):
    '''
    Fetches embeddings from the database
    - enables filtering by where_filter, sorting by key, and limiting the number of results
    '''
    return app._db.fetch(where_filter, sort, limit).to_dict(orient="records")

@app.get("/api/v1/count")
async def count():
    '''
    Returns the number of records in the database
    '''
    return ({"count": app._db.count()})

@app.get("/api/v1/persist")
async def persist():
    '''
    Persist the database and index to disk
    '''
    if not os.path.exists(".chroma"):
        os.mkdir(".chroma")
        
    app._db.persist()
    app._ann_index.persist()
    return True

@app.get("/api/v1/reset")
async def reset():
    '''
    Reset the database and index
    '''
    shutil.rmtree(".chroma", ignore_errors=True)
    app._db = db()
    app._ann_index = ann_index()
    return True

@app.get("/api/v1/rand")
async def rand(where_filter={}, sort=None, limit=None):
    '''
    Randomly bisection the database
    '''
    results = app._db.fetch(where_filter, sort, limit)
    rand = rand_bisectional_subsample(results)
    return rand.to_dict(orient="records")

@app.post("/api/v1/get_nearest_neighbors")
async def get_nearest_neighbors(embedding: QueryEmbedding):
    '''
    return the distance, database ids, and embedding themselves for the input embedding
    '''
    ids = None
    filter_by_where = {}
    if embedding.category_name is not None:
        filter_by_where['category_name'] = embedding.category_name
    if embedding.dataset is not None:
        filter_by_where['dataset'] = embedding.dataset

    if filter_by_where is not None:
        ids = app._db.fetch(filter_by_where)["id"].tolist()
    
    nn = app._ann_index.get_nearest_neighbors(embedding.embedding, embedding.n_results, ids)
    return {
        "ids": nn[0].tolist()[0],
        "embeddings": app._db.get_by_ids(nn[0].tolist()[0]).to_dict(orient="records"),
        "distances": nn[1].tolist()[0]
    }