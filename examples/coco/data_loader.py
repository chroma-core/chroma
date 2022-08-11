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

project = nn(chroma.create_or_get_project('cat data later6'))

ann_file = "/Users/jeff/data/annotations/instances_train2014.json"
coco=COCO(ann_file)
 
# Get list of category_ids, here [2] for bicycle
# category_ids = coco.getCatIds(['bicycle'])
cat_ids = coco.getCatIds()
cats = coco.loadCats(cat_ids)
dataset = nn(chroma.create_or_get_dataset("cat data later6", int(project.createOrGetProject.id)))
embedding_set = nn(chroma.create_embedding_set(int(dataset.createOrGetDataset.id)))

chroma.update_dataset(int(dataset.createOrGetDataset.id), None, json.dumps(cats))

# Get list of image_ids which contain bicycles
# image_ids = coco.getImgIds(catIds=[2])
image_ids = coco.getImgIds()
print(str(len(image_ids)))

str_options = ['New York', 'San Francisco', 'Atlanta', 'Miami', 'Dallas', 'Chicago', 'DC']

i = 0
add_data_batch = []
for image_id in image_ids:
    # print(str(i))
    # create a datapoint, resource, label...... 
    # where resource is the image uri
    # and the label is the coco json associated with that image
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

    # create_datapoint_set = nn(chroma.create_datapoint_set(int(dataset.createOrGetDataset.id), json.dumps(data), uri))
    # print(str(i))
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

    # create_datapoint_set = nn(chroma.create_datapoint_set(int(dataset.createOrGetDataset.id), json.dumps(data), uri))

    add_data_batch.append(data_item)
    # print(str(create_datapoint_set))
    if (i == (len(image_ids)-1)): 
        chroma.create_batch_datapoint_embedding_set(add_data_batch)
        print (str(i))

    if(not i % 1_000):
        chroma.create_batch_datapoint_embedding_set(add_data_batch)
        add_data_batch = []
        print (str(i))
        
    if (i > 1_100):
        raise Exception("stop")
