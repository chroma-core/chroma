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
        self.data["embedding_width"] = 85

    @property
    def embedding_width(self):
        return self.data["embedding_width"]

    @property
    def embedding(self):
        if not self.data.get("detection"):
            return [0.0] * self.embedding_width
        return self.data["detection"]["embedding"]

    @property
    def embedding_as_dict(self):
        column_names = [f"embedding_{i}" for i in range(self.embedding_width)]
        return {
            f"embedding_{i}": value for i, value in zip(column_names, self.embedding)
        }

    @property
    def row(self):
        label_bbox = (self.data.get("label") or {}).get("bbox", [0, 0, 0, 0])
        detection_bbox = (self.data.get("detection") or {}).get("bbox", [0, 0, 0, 0])
        row = {
            "input_uri": self.data["input_uri"],
            "layer": self.data["layer"],
            "detection_id": (self.data.get("detection") or {}).get("id"),
            "detection_category_id": (self.data.get("detection") or {}).get(
                "category_id"
            ),
            "detection_category_name": (self.data.get("detection") or {}).get(
                "category_name"
            ),
            "detection_iou": (self.data.get("detection") or {})
            .get("metadata", {})
            .get("iou"),
            "detection_score": (self.data.get("detection") or {})
            .get("metadata", {})
            .get("score"),
            "detection_x": detection_bbox[0],
            "detection_y": detection_bbox[1],
            "detection_width": detection_bbox[2],
            "detection_height": detection_bbox[3],
            "label_id": (self.data.get("label") or {}).get("id"),
            "label_category_id": (self.data.get("label") or {}).get("category_id"),
            "label_category_name": (self.data.get("label") or {}).get("category_name"),
            "label_x": label_bbox[0],
            "label_y": label_bbox[1],
            "label_width": label_bbox[2],
            "label_height": label_bbox[3],
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

for data in stream_json("object.jsonl.bz2"):
    buffer.add(Object(data))

pd_table = buffer.realize()
print(pd_table)
pa_table = pa.Table.from_pandas(pd_table)
print(pa_table)

pq.write_table(pa_table, "coco_object.parquet")
