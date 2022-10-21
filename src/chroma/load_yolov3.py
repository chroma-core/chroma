
from chroma import Chroma
import pyarrow.parquet as pq
import numpy as np
from pandas.io.json import json_normalize
import json
import time

if __name__ == "__main__":

    py = pq.read_table('data/objects_data_recorder_fixed.parquet')

    df = py.to_pandas()

    len = len(df)

    base_metadata = {
        "app":"yolov3", 
        "model_version":"1.0.0", 
        "layer":"pool5", 
    }

    chroma = Chroma(base_metadata=base_metadata)
    allstart = time.time()
    start = time.time()
    BATCH_REPORTING_SIZE = 100_000
    for index, row in df.iterrows():
        # this turns numpy arrays into lists so that they can be json serialized, yes this is my workaround
        for element in row['infer']['annotations']:
            element['bbox'] = element['bbox'].tolist()
        row['infer']['annotations'] = row['infer']['annotations'].tolist()
        embeddings=row['embedding_data'].tolist()

        # if index is divislbe by 1000, print the index
        if index % BATCH_REPORTING_SIZE == 0:
            end = time.time()
            print("time to log n single record: ", "{:.2f}".format(end - start), index, "n=", BATCH_REPORTING_SIZE)
            start = time.time()
        
        if (index < len/2):
            chroma.log_training(
                input_uri=row['resource_uri'],
                inference_data=row['infer'], # perhaps we should change this input to get away from COCO formatting reliance
                embedding_data=embeddings)
        else: 
            chroma.log_production(
                input_uri=row['resource_uri'],
                inference_data=row['infer'], # perhaps we should change this input to get away from COCO formatting reliance
                embedding_data=embeddings)

    allend = time.time()
    print("time to log all: ", "{:.2f}".format(allend - allstart))

    print(chroma.fetch())

    chroma.process()

    highest_signal = chroma.fetch_highest_signal()
    for index, row in highest_signal.iterrows():
        print(row['input_uri'], row['infer'], row['distance'])

    del chroma