#!/usr/bin/env python3

import bz2
import json
import re

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


coco_context_nested = pq.read_table("coco_context_nested.parquet")
ctx_nested = coco_context_nested.to_pandas()
print(ctx_nested)

filter1 = ctx_nested[(ctx_nested.input_uri == "val2014/COCO_val2014_000000000164.jpg")]
print(filter1)


def chair_filter(raw_objects):
    ptable = pd.DataFrame(raw_objects)
    print("FILTER")
    print(ptable)
    return True
    for object in json.loads(raw_objects):
        # print("FILTER:", object)
        detection = object.get("detection", None) or {}
        # print("FILTER:", detection)
        category_name = detection.get("category_name")
        # print("FILTER:", category_name)
        if category_name == "chair":
            return True
    return False


obj1_bools = filter1.objects.apply(lambda raw: chair_filter(raw))
print("Filter on first record:", obj1_bools.to_list())

# ctx_nested_bools = ctx_nested.objects.apply(lambda raw: chair_filter(raw))
# # print("Filter on full set:", ctx_nested_bools.to_list())
# full_chair = ctx_nested[ctx_nested_bools]

# print("All chairs", full_chair)
