import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from collections import defaultdict
from pathlib import Path


class Parquet:
    stage = defaultdict(list)
    training_embeddings = defaultdict(list)
    prod_embeddings = []

    inferences = defaultdict(list)

    @property
    def index_path(self):
        return self.scratch_path / "parquet"

    def path_for(self, model_key):
        return str((self.index_path / model_key).with_suffix(".parquet"))

    def init_db(self, scratch_path):
        self.scratch_path = Path(scratch_path)
        self.index_path.mkdir(parents=True, exist_ok=True)

    def ingest_prod(self, embedding):
        self.prod_embeddings.append(embedding)

    def ingest_training(self, embedding):
        self.stage[embedding.key].append(embedding)

    def training_counts(self):
        return [(cat, len(embeds)) for cat, embeds in self.training_embeddings.items()]

    def categories(self):
        return self.inferences.keys()

    def embeddings_for_category(self, category):
        return self.inferences[category]

    def commit(self):
        stage = self.stage
        self.stage = {}

        for key, embeddings in stage.items():
            model, mode = key
            table = np.array([e.data for e in embeddings])
            infs = [e.inference for e in embeddings]
            width = table.shape[1]
            twist = table.transpose()
            print("shape", twist.shape, "width", width)
            np_cols = [twist[i, :] for i in range(width)]
            pa_cols = [pa.array(c) for c in np_cols]
            data = {
                f"col{i}": pa_cols[i]
                for i in range(width)
            }
            data['inferences'] = infs
            pa_table = pa.Table.from_pydict(data)
            pq.write_table(pa_table, self.path_for(model))
        
            # pretend round trip
            pq_table = pq.read_table(self.path_for(model))
            round_np = np.array(pq_table)
            round_rows = round_np.transpose().tolist()
            print("round", round_np.shape, "cols", len(pq_table.columns), len(round_rows))

            for row in round_rows:
                # super gross test hack
                embedding = row[:10]
                inference = int(row[-1])
                self.inferences[inference].append(embedding)

            # brute force iterate, but I think I can replace this with one pq batch per inference
            # inferences = {}
            # for row in round_rows:
            #     inferences[]


            # for embedding in embeddings:
            #     self.training_embeddings[model].append(embedding)
