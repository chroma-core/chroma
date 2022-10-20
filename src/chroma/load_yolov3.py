
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

    for index, row in df.iterrows():
        # this turns numpy arrays into lists so that they can be json serialized, yes this is my workaround
        for element in row['infer']['annotations']:
            element['bbox'] = element['bbox'].tolist()
        row['infer']['annotations'] = row['infer']['annotations'].tolist()
        embeddings=row['embedding_data'].tolist()
        
        if (index < len/2):
            chroma.log_training(
                input_uri=row['resource_uri'],
                inference_data=json.dumps(row['infer']), # perhaps we should change this input to get away from COCO formatting reliance
                embedding_data=embeddings)
        else: 
            chroma.log_production(
                input_uri=row['resource_uri'],
                inference_data=json.dumps(row['infer']), # perhaps we should change this input to get away from COCO formatting reliance
                embedding_data=embeddings)

    print(chroma.fetch())

    chroma.process()

    highest_signal = chroma.fetch_highest_signal()
    for index, row in highest_signal.iterrows():
        print(row['input_uri'], row['infer'], row['distance'])

    del chroma