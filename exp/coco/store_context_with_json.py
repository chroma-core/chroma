#!/usr/bin/env python3

import bz2
import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class Object:
    def __init__(self, data):
        self.data = data
        self.data["embedding_width"] = len(self.data["embedding"])

    @property
    def embedding_width(self):
        return self.data["embedding_width"]

    @property
    def embedding(self):
        return self.data["embedding"]

    @property
    def embedding_as_dict(self):
        column_names = [f"embedding_{i:04}" for i in range(self.embedding_width)]
        return {key: value for key, value in zip(column_names, self.embedding)}

    @property
    def row(self):
        label_bbox = (self.data.get("label") or {}).get("bbox", [0, 0, 0, 0])
        detection_bbox = (self.data.get("detection") or {}).get("bbox", [0, 0, 0, 0])
        row = {
            "input_uri": self.data["input_uri"],
            "layer": self.data["layer"],
            "objects": json.dumps(self.data["objects"]),
        }
        row.update(self.embedding_as_dict)
        return row


class ObjectBuffer:
    objects = []

    def add(self, object):
        self.objects.append(object)

    def realize(self):
        objects = self.objects
        self.objects = []

        grid = [o.row for o in objects]
        df = pd.DataFrame(grid)
        return df


def stream_json(filename):
    with bz2.open(filename, "r") as file:
        count = 0
        for row in file:
            yield json.loads(row)
            count += 1
            # if count >= 100:
            #     break


buffer = ObjectBuffer()

for data in stream_json("context.jsonl.bz2"):
    buffer.add(Object(data))

pd_table = buffer.realize()
print(pd_table)
pa_table = pa.Table.from_pandas(pd_table)
print(pa_table)

pq.write_table(pa_table, "coco_context_with_json.parquet")
