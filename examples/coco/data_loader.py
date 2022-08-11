from PIL import Image
import random
from matplotlib import image
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pycocotools.coco import COCO
from chroma.sdk import chroma_manager
from chroma.sdk.utils import nn
import json

chroma = chroma_manager.ChromaSDK(project_name= 'COCO', dataset_name='Train2014')

project = nn(chroma.create_or_get_project('COCO Data'))

ann_file = "/Users/jeff/data/annotations/instances_train2014.json"
coco=COCO(ann_file)
 
cat_ids = coco.getCatIds()
cats = coco.loadCats(cat_ids)
dataset = nn(chroma.create_or_get_dataset("COCO Data", int(project.createOrGetProject.id)))
embedding_set = nn(chroma.create_embedding_set(int(dataset.createOrGetDataset.id)))

chroma.update_dataset(int(dataset.createOrGetDataset.id), None, json.dumps(cats))

image_ids = coco.getImgIds()
print(str(len(image_ids)))

str_options = ['New York', 'San Francisco', 'Atlanta', 'Miami', 'Dallas', 'Chicago', 'DC']

i = 0
add_data_batch = []
for image_id in image_ids:
    uri = "/Users/jeff/data/train2014/COCO_train2014_" + str(image_id).zfill(12)+".jpg"

    annotation_ids = coco.getAnnIds(imgIds=image_id)
    anns = coco.loadAnns(annotation_ids)

    anns_trimmed = []
    for ann in anns:
        anns_trimmed.append({
            'iscrowd': ann['iscrowd'], 
            'image_id': ann['image_id'], 
            'bbox': ann['bbox'], 
            'category_id': ann['category_id'], 
            'id':ann['id'], 
        })

    data = {
        'annotations': anns_trimmed
    }

    i = i + 1

    data_item = {
        'datasetId': int(dataset.createOrGetDataset.id), 
        'labelData': json.dumps(data), 
        'inferenceData': json.dumps({}), 
        'resourceUri': uri, 
        'embeddingData': json.dumps({}), 
        'embeddingSetId': int(embedding_set.createEmbeddingSet.id), 
        'metadata': json.dumps(
            {
                'quality': random.randint(0, 100),
                'location': str_options[random.randint(0, 6)]
            }
        )
    }

    add_data_batch.append(data_item)

    if (i == (len(image_ids)-1)): 
        chroma.create_batch_datapoint_embedding_set(add_data_batch)
        print (str(i))

    if(not i % 1_000):
        chroma.create_batch_datapoint_embedding_set(add_data_batch)
        add_data_batch = []
        print (str(i))
        
    # if (i > 1_100):
    #     raise Exception("stop")
