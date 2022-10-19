import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path

from .dataset import Dataset

class ParquetDataset(Dataset):
    def __init__(self, fullpath:str):
        self.fullpath = str(fullpath)

    @property
    def name(self):
        return Path(self.fullpath).name

    @property
    def parquet_table(self):
        return pq.read_table(self.fullpath)

    def iterate(self):
        table = self.parquet_table.to_pandas()
        for row in table.T:
            yield row

    def __repr__(self):
        return f"ParquetDataset({self.name})"
