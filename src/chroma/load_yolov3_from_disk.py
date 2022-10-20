
from chroma import Chroma
import pyarrow.parquet as pq
import numpy as np
from pandas.io.json import json_normalize
import json

if __name__ == "__main__":

    py = pq.read_table('data/yolov3_objects.parquet')

    df = py.to_pandas()

    len = len(df)

    base_metadata = {
        "app":"yolov3", 
        "model_version":"1.0.0", 
        "layer":"pool5", 
    }

    chroma = Chroma(base_metadata=base_metadata)

    highest_signal = chroma.fetch_highest_signal()
    for index, row in highest_signal.iterrows():
        print(row['input_uri'], row['infer'], row['distance'])