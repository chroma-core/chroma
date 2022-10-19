import logging

from typing import Union
from fastapi import Depends, FastAPI

from pathlib import Path

import db.adapters

from config import get_settings, Settings
from model.dataset.parquet import ParquetDataset
from pipeline.indexer.mhb import MHBIndexer
from service.service import Services

logger = logging.getLogger(__name__)
services = Services(
    dataset=ParquetDataset, 
    mhb_indexer=MHBIndexer,
)

app = FastAPI(dependencies=[Depends(get_settings)])

data = services.dataset(Path(get_settings().dataset_path) / "objects_data_recorder_fixed.parquet")
#data = ParquetDataset(Path(get_settings().dataset_path) / "objects_data_recorder_fixed.parquet")
logger.warning("DATA %s", data)
count = 0
for chunk in data.iterate():
    count += 1
print("  rows: %s", count)

@app.get("/")
async def read_root(
    settings: Settings = Depends(get_settings),
    services: Services = Depends(Services.Self),
):
    print("settings", settings)
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/build_index")
async def build_index(settings: Settings = Depends(get_settings)):
    print("settings", settings)
    return {"Hello": "World"}
