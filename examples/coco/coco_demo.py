from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.patches as patches

from pycocotools.coco import COCO

ann_file = "/Users/jeff/data/annotations/instances_train2014.json"
coco=COCO(ann_file)
 
# Get list of category_ids, here [2] for bicycle
category_ids = coco.getCatIds(['bicycle'])

# Get list of image_ids which contain bicycles
image_ids = coco.getImgIds(catIds=[2])
print(str(image_ids))
 
image_id = image_ids[0]
 
images_path = "/Users/jeff/data/train2014/COCO_train2014_"
image_name = str(image_id).zfill(12)+".jpg" # Image names are 12 characters long
image = Image.open(images_path+image_name)
 
fig, ax = plt.subplots()

annotation_ids = coco.getAnnIds(imgIds=image_id)#, catIds=category_ids[0])
anns = coco.loadAnns(annotation_ids)
print(str(anns))
# Draw boxes and add label to each box
for ann in anns:
    box = ann['bbox']
    bb = patches.Rectangle((box[0],box[1]), box[2],box[3], linewidth=2, edgecolor="blue", facecolor="none")
    ax.add_patch(bb)
 
ax.imshow(image)
plt.show()