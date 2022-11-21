import os
from fastapi import FastAPI

from chroma_server.routes import ChromaRouter
from chroma_server.index.hnswlib import Hnswlib
from chroma_server.db.duckdb import DuckDB

# we import types here so that the user can import them from here
from chroma_server.types import (
    ProcessEmbedding, AddEmbedding, FetchEmbedding, 
    QueryEmbedding, CountEmbedding, DeleteEmbedding, 
    RawSql, Results, SpaceKeyInput)

core = FastAPI(debug=True)
core._db = DuckDB()
core._ann_index = Hnswlib()

router = ChromaRouter(app=core, db=DuckDB, ann_index=Hnswlib)
core.include_router(router.router)

def init(filesystem_location: str = None):
    if filesystem_location is None:
        filesystem_location = os.getcwd()

    # create a dir
    if not os.path.exists(filesystem_location + '/.chroma'):
        os.makedirs(filesystem_location + '/.chroma')
    
    if not os.path.exists(filesystem_location + '/.chroma/index_data'):
        os.makedirs(filesystem_location + '/.chroma/index_data')
    
    # specify where to save and load data from 
    core._db.set_save_folder(filesystem_location + '/.chroma')
    core._ann_index.set_save_folder(filesystem_location + '/.chroma/index_data')

    print("Initializing Chroma...")
    print("Data will be saved to: " + filesystem_location + '/.chroma')

    # if the db exists, load it
    if os.path.exists(filesystem_location + '/.chroma/chroma.parquet'):
        print(f"Existing database found at {filesystem_location + '/.chroma/chroma.parquet'}. Loading...")
        core._db.load()

core.init = init

# headless mode
core.heartbeat = router.root
core.add = router.add
core.count = router.count
core.fetch = router.fetch
core.reset = router.reset
core.delete = router.delete
core.get_nearest_neighbors = router.get_nearest_neighbors
core.raw_sql = router.raw_sql
core.create_index = router.create_index

# these as currently constructed require celery
# chroma_core.process = process
# chroma_core.get_status = get_status
# chroma_core.get_results = get_results
