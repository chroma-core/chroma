
from chroma import Chroma
import pyarrow.parquet as pq
import numpy as np
from pandas.io.json import json_normalize
import json
import time

if __name__ == "__main__":
    

    py = pq.read_table('data/objects_data_recorder_fixed.parquet')

    df = py.to_pandas()
    # print(df.head())

    base_metadata = {
        "app":"yolov3", 
        "model_version":"1.0.0", 
        "layer":"pool5", 
    }

    chroma = Chroma(base_metadata=base_metadata)

    allstart = time.time()
    programstart = time.time()
    BATCH_SIZE = 100_000
    dflength = len(df)
    for i in range(0, dflength, BATCH_SIZE):
        start = time.time()
        batch = df[i:i+BATCH_SIZE]

        chroma.log_training(
                input_uri=batch['resource_uri'].tolist(),
                inference_data=batch['infer'].tolist(),
                embedding_data=batch['embedding_data'].tolist()
            )

        end = time.time()
        print("time to log batch: ", "{:.2f}".format(end - start), i+BATCH_SIZE)

    allend = time.time()
    print("time to log all: ", "{:.2f}".format(allend - allstart))

    print("fetch the data", str(chroma.fetch()))

    chroma.process()

    programend = time.time()
    print("program ", "{:.2f}".format(programend - programstart))