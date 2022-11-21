import time
import os

from chroma_server.db.clickhouse import Clickhouse
from chroma_server.db.duckdb import DuckDB
from chroma_server.index.hnswlib import Hnswlib
from chroma_server.utils.error_reporting import init_error_reporting
from chroma_server.utils.telemetry.capture import Capture
from fastapi import FastAPI

chroma_telemetry = Capture()
chroma_telemetry.capture('server-start')
init_error_reporting()

from chroma_server.routes import ChromaRouter

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

if chroma_mode == 'in-memory':
    filesystem_location = os.getcwd()

    # create a dir
    if not os.path.exists(filesystem_location + '/.chroma'):
        os.makedirs(filesystem_location + '/.chroma')
    
    if not os.path.exists(filesystem_location + '/.chroma/index_data'):
        os.makedirs(filesystem_location + '/.chroma/index_data')
    
    # specify where to save and load data from 
    app._db.set_save_folder(filesystem_location + '/.chroma')
    app._ann_index.set_save_folder(filesystem_location + '/.chroma/index_data')

    print("Initializing Chroma...")
    print("Data will be saved to: " + filesystem_location + '/.chroma')

    # if the db exists, load it
    if os.path.exists(filesystem_location + '/.chroma/chroma.parquet'):
        print(f"Existing database found at {filesystem_location + '/.chroma/chroma.parquet'}. Loading...")
        app._db.load()

router = ChromaRouter(app=app, db=db, ann_index=ann_index)
app.include_router(router.router)
