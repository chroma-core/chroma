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

def create_index_data_dir():
    if not os.path.exists(os.getcwd() + '/index_data'):
        os.makedirs(os.getcwd() + '/index_data')
    app._ann_index.set_save_folder(os.getcwd() + '/index_data')

if chroma_mode == 'in-memory':
    create_index_data_dir()

router = ChromaRouter(app=app, db=db, ann_index=ann_index)
app.include_router(router.router)
