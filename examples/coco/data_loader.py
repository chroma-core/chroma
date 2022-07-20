from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pycocotools.coco import COCO
from chroma.sdk import chroma_manager
from chroma.sdk.utils import nn
import json

chroma = chroma_manager.ChromaSDK(project_name= 'COCO', dataset_name='Train2014')

project = nn(chroma.create_or_get_project('COCO'))
dataset = nn(chroma.create_or_get_dataset("Train2014", int(project.createOrGetProject.id)))

ann_file = "/Users/jeff/data/annotations/instances_train2014.json"
coco=COCO(ann_file)
 
# Get list of category_ids, here [2] for bicycle
category_ids = coco.getCatIds(['bicycle'])

# Get list of image_ids which contain bicycles
image_ids = coco.getImgIds(catIds=[2])

for image_id in image_ids:
    # create a datapoint, resource, label...... 
    # where resource is the image uri
    # and the label is the coco json associated with that image
    uri = "/Users/jeff/data/train2014/COCO_train2014_" + str(image_id).zfill(12)+".jpg"

    annotation_ids = coco.getAnnIds(imgIds=image_id)
    anns = coco.loadAnns(annotation_ids)

    cat_ids = coco.getCatIds()
    cats = coco.loadCats(cat_ids)

    data = {
        'categories': cats,
        'annotations': anns
    }

    print(str(data))

    create_datapoint_set = nn(chroma.create_datapoint_set(int(dataset.createOrGetDataset.id), json.dumps(data), uri))
    print(str(create_datapoint_set))
