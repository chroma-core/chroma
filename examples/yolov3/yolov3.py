# chroma stuff
from chroma_client import Chroma
import pyarrow.parquet as pq
import numpy as np
from pandas.io.json import json_normalize
import json
import time
import os
import pandas as pd
import uuid

###############
an_embedding = None

DATA_MODE = "chroma"

if __name__ == "__main__":

    # weaviate stuff
    if DATA_MODE == "weaviate":
        import weaviate
        import uuid
        client = weaviate.Client("http://localhost:8080") # or another location where your Weaviate instance is running
        client.batch.configure(
        # `batch_size` takes an `int` value to enable auto-batching
        # (`None` is used for manual batching)
        batch_size=10_000, 
        # dynamically update the `batch_size` based on import speed
        dynamic=False,
        # `timeout_retries` takes an `int` value to retry on time outs
        timeout_retries=3,
        # checks for batch-item creation errors
        # this is the default in weaviate-client >= 3.6.0
        callback=weaviate.util.check_batch_result,
        )
        result = client.batch.delete_objects(
            class_name='EmbeddingRow',
            output='verbose',
            dry_run=False,
            where={
                'operator': 'Equal',
                'path': ['dataset'],
                'valueString': 'training'
            },
        )
        print(result)
        all_objects = client.data_object.get(class_name="EmbeddingRow")
        print("all_objects", len(all_objects), all_objects)

    if DATA_MODE == "milvus":
        from pymilvus import (
            connections,
            utility,
            FieldSchema,
            CollectionSchema,
            DataType,
            Collection,
        )
        connections.connect("default", host="localhost", port="19530")
        fields = [
            FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name="uuid", dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(name="dataset", dtype=DataType.VARCHAR, max_length=64),
            # FieldSchema(name="model_space", dtype=DataType.VARCHAR),
            FieldSchema(name="input_uri", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="inference_class", dtype=DataType.VARCHAR, max_length=64),
            # FieldSchema(name="label_class", dtype=DataType.VARCHAR),
            FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=85)
        ]
        schema = CollectionSchema(fields, "hello_milvus is the simplest demo to introduce the APIs")
        hello_milvus = Collection("hello_milvus7", schema)


    # file = 'data__nogit/yolov3_objects_large_5k.parquet'
    file = 'data__nogit/yolov3_objects_large.parquet'

    print("Loading parquet file: ", file)
    py = pq.read_table(file)
    df = py.to_pandas()
    print("Number of records: ", len(df))

    data_length = len(df)

    if DATA_MODE == "chroma":
        chroma = Chroma(model_space="yolov3")
        chroma.reset() #make sure we are using a fresh db
    allstart = time.time()
    start = time.time()

    dataset = "training"
    BATCH_SIZE = 10_000

    print("Loading in records with a batch size of: " , data_length)

    for i in range(0, data_length, BATCH_SIZE):
        if i >= 30_000:
            break

        end = time.time()
        page = i * BATCH_SIZE
        print("Time to process BATCH_SIZE rows: " + '{0:.2f}'.format((end - start)) + "s, records loaded: " + str(i))
        start = time.time()

        batch = df[i:i+BATCH_SIZE]

        for index, row in batch.iterrows():
            for idx, annotation in enumerate(row['infer']['annotations']):
                annotation["bbox"] = annotation['bbox'].tolist()
                row['infer']['annotations'] = row['infer']['annotations'].tolist()

            row['embedding_data'] = row['embedding_data'].tolist()
            an_embedding = row['embedding_data']

        embedding = batch['embedding_data'].tolist()
        input_uri = batch['resource_uri'].tolist()

        inference_classes = []
        for index, row in batch.iterrows():
            for idx, annotation in enumerate(row['infer']['annotations']):
                inference_classes.append(annotation['category_name'])

        datasets = dataset
        

        # if data_mode = chroma
        if DATA_MODE == "chroma":
            chroma.add(
                embedding=embedding, 
                input_uri=input_uri, 
                dataset=dataset,
                inference_class=inference_classes
            )

        if DATA_MODE == "milvus":
            milvus_datasets = [dataset] * len(embedding)
            entities = [
                [i+BATCH_SIZE for i in range(BATCH_SIZE)],  # field pk
                [str(uuid.uuid4()) for i in range(BATCH_SIZE)],  # field uuid
                milvus_datasets,  
                input_uri,
                inference_classes,
                embedding
            ]
            insert_result = hello_milvus.insert(entities)

        # if data_mode = weaviate
        if DATA_MODE == "weaviate":
            with client.batch as weaviate_batch:
                for index, row in batch.iterrows():
                    weaviate_batch.add_data_object(
                        {
                            "dataset": dataset,
                            "model_space": "yolov3",
                            "input_uri": row['resource_uri'],
                            "inference_class": "knife",
                            # "label_class":label_class
                        },
                        'EmbeddingRow',
                        vector=row['embedding_data']
                    )   

                # batch.add_data_object(first_object_props, 'EmbeddingRow', "36ddd591-2dee-4e7e-a3cc-eb86d30a4303", vector=[0.1, 0.2, 0.3])

    if DATA_MODE == "weaviate":
        all_objects = client.data_object.get(class_name="EmbeddingRow")
        print("objects in milvus", len(all_objects))        

    allend = time.time()
    print("time to add all: ", "{:.2f}".format(allend - allstart) + 's')

    if DATA_MODE == "weaviate":
        nearVector = {"vector": an_embedding}
        # nearVector = {"vector": [-0.36840257,0.13973749,-0.28994447,-0.18607682,0.20019795,0.15541431,-0.42353877,0.30262852,0.2724561,0.07069917,0.4877447,0.038771532,0.64523,-0.15907241,-0.3413626,-0.026682584,-0.63310874,-0.33411884,0.082939014,0.30305764,0.045918174,-0.21439327,-0.5005205,0.6210859,-0.2729049,-0.51221114,0.09680918,0.094923325,-0.15688285,-0.07325482,0.6588305,0.0523736,-0.14173415,-0.27428055,0.25526586,0.057506185,-0.3103442,0.028601522,0.124522656,0.66984487,0.12160647,-0.5090515,-0.540393,-0.39546522,-0.2201204,0.34625968,-0.21068871,0.21132985,0.048714135,0.09043683,0.3176081,-0.056684002,-0.12117501,-0.6591976,-0.26731065,0.42615625,0.33333477,-0.3240578,-0.18771006,0.2328068,-0.17239179,-0.33583146,-0.6556605,-0.10608161,-0.5135395,-0.25123677,-0.23004892,0.7036331,0.04456794,0.41253626,0.27872285,-0.28226635,0.11927197,-0.4677766,0.4343466,-0.17538455,0.10621233,0.95815116,0.23587844,-0.006406698,-0.10512518,-1.1125883,-0.37921682,0.040789194,0.676718,0.3369762,0.040712647,0.580487,0.20063736,-0.021220192,-0.09071747,-0.0023735985,0.30007777,-0.039925132,0.4035474,-0.2518212,-0.17846306,0.12371392,-0.0703354,-0.3752431,-0.652917,0.5952828,1.3426708,-0.08167235,-0.38515738,0.058423538,-0.08100355,-0.192886,0.3745164,-0.23291737,0.33326542,-0.6019264,-0.42822492,-0.6524583,-0.15210791,-0.5073593,0.022548754,-0.058033653,-0.47369233,-0.30890635,0.6338296,0.0017854869,0.1954949,0.99348027,-0.26558784,-0.058124136,1.149388,0.02915948,0.013422121,0.25484946,-0.030017598,-0.23879935,0.053123385,-0.36463016,-0.0024245526,0.1202083,-0.45966506,-0.34140104,-0.08484162,-0.03537422,-0.2817959,0.25044164,-0.5060605,0.1252808,-0.032539487,0.110069446,-0.20679846,-0.46421885,-0.4141739,0.26994973,-0.070687145,0.16862138,-0.20162229,0.22199251,-0.2771402,0.23653336,0.16585203,-0.08286354,-0.15343396,0.23893964,-0.7453282,-0.16549355,-0.1947069,0.46136436,0.22064126,0.28654936,-0.038697664,0.037633028,-0.80988157,0.5094175,-0.0920082,0.25405347,-0.64169943,0.43366328,-0.2999211,-0.4090591,0.11957859,0.00803617,-0.0433745,0.12818244,0.28464508,-0.31760025,0.16558012,-0.33553946,-0.3943465,0.59569097,-0.6524206,0.3683173,-0.60456693,0.2046492,0.46010277,0.24695799,0.2946015,0.11376746,-0.027988048,0.03749422,-0.16577742,0.23407385,-0.0231737,-0.023245076,0.08752677,0.2299883,0.35467404,0.046193745,-0.39828986,0.21079691,0.38396686,-0.0018698421,0.16047359,-0.057517264,-0.203534,0.23438136,-0.84250915,0.22371331,0.0058325706,0.30733636,0.19518353,-0.108008966,0.6509316,0.070131645,-0.24023099,0.28779706,0.2326336,0.07004021,-0.45955566,0.20426086,-0.37472793,-0.049604423,0.4537271,0.6133582,-1.0527759,-0.5472505,0.15193434,0.5296606,-0.11560251,0.07279209,0.40557706,0.2505283,0.24490519,0.017602902,-0.004647707,0.16608049,0.12576887,0.118216865,0.4403996,0.39552462,-0.22196701,-0.061155193,0.03693534,-0.4022908,0.3842317,-0.0831345,0.01930883,0.3446575,-0.2167439,-0.23994556,-0.09370326,-0.3671856,0.044011243,0.017895095,-0.019855855,-0.16416992,0.17858285,0.31287143,0.38368022,-0.006513525,0.45780763,-0.23027879,0.108570844,-0.4449492,-0.035763215,0.03818417,0.040017277,-0.17022872,-0.2622464,0.65610534,0.16720143,0.2515769,-0.23535803,0.62484455,0.16771325,-0.62404263,0.19176348,-0.72786695,0.18485649,-0.30914405,-0.3230534,-0.24064465,0.28841522,0.39792386,0.15618932,0.03928854,0.18277727,-0.101632096,0.1868196,-0.33366352,0.086561844,0.48557812,-0.6198209,-0.07978742]}
        res = client.query.get(class_name='EmbeddingRow', properties="input_uri").with_near_vector(nearVector).do() # note that certainty is only supported if distance==cosine
        print("result!", res)


    # fetched = chroma.count()
    # print("Records loaded into the database: ",  fetched)

    start = time.time()

    if DATA_MODE == "milvus":
        index = {
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "params": {"nlist": 128},
        }
        hello_milvus.create_index("embeddings", index)

    if DATA_MODE == "chroma":
        chroma.create_index()
    end = time.time()
    print("Time to process: "  +'{0:.2f}'.format((end - start)) + 's')

    # knife_embedding = [0.2310010939836502, -0.3462161719799042, 0.29164767265319824, -0.09828940033912659, 1.814868450164795, -10.517369270324707, -13.531850814819336, -12.730537414550781, -13.011675834655762, -10.257010459899902, -13.779699325561523, -11.963963508605957, -13.948140144348145, -12.46799087524414, -14.569470405578613, -16.388280868530273, -13.76762580871582, -12.192169189453125, -12.204055786132812, -12.259000778198242, -13.696036338806152, -14.609177589416504, -16.951879501342773, -17.096384048461914, -14.355693817138672, -16.643482208251953, -14.270745277404785, -14.375198364257812, -14.381218910217285, -13.475995063781738, -12.694938659667969, -10.011992454528809, -9.770626068115234, -13.155019760131836, -16.136341094970703, -6.552414417266846, -11.243837356567383, -16.678457260131836, -14.629229545593262, -10.052337646484375, -15.451828956604004, -12.561151504516602, -11.68396282196045, -11.975972175598145, -11.09926986694336, -13.060500144958496, -12.075592994689941, -1.0808746814727783, 1.7046797275543213, -3.8080708980560303, -11.401922225952148, -12.184720039367676, -13.262567520141602, -11.299583435058594, -13.654638290405273, -10.767330169677734, -9.012763977050781, -10.202326774597168, -10.088111877441406, -13.247991561889648, -9.651527404785156, -11.903244972229004, -13.922954559326172, -17.37179946899414, -12.51513385772705, -7.8046746253967285, -14.406414985656738, -13.172696113586426, -11.194984436035156, -12.029500961303711, -10.996524810791016, -10.828441619873047, -8.673471450805664, -13.800869941711426, -9.680946350097656, -12.964024543762207, -9.694372177124023, -13.132003784179688, -9.38864803314209, -14.305071830749512, -14.4693603515625, -5.0566205978393555, -15.685358047485352, -12.493011474609375, -8.424881935119629]

    # start = time.time()
    # get_nearest_neighbors = chroma.get_nearest_neighbors(knife_embedding, 4, where_filter= {"inference_class": "knife","dataset": "training"})
    # print("get_nearest_neighbors: ", get_nearest_neighbors)
    # res_df = pd.DataFrame(get_nearest_neighbors['embeddings'])
    # print(res_df.head())

    # print("Distances to nearest neighbors: ", get_nearest_neighbors['distances'])
    # print("Internal ids of nearest neighbors: ", get_nearest_neighbors['ids'])

    # end = time.time()
    # print("Time to get nearest neighbors: " +'{0:.2f}'.format((end - start)) + 's')

    # fetched = chroma.count()
    # print("Records loaded into the database: ",  fetched)
    # del chroma
