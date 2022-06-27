import resource
from venv import create
from chroma.cli.sdk import chroma_manager
from chroma.sdk.utils import nn

print("running seeds.py")

chroma = chroma_manager.ChromaSDK()

project = nn(chroma.create_project("my first project"))

# data stuff
dataset1 = nn(chroma.create_dataset("training", int(project.createProject.id)))
dataset2 = nn(chroma.create_dataset("production", int(project.createProject.id)))

dataset3 = nn(chroma.create_or_get_dataset("dontdupe", int(project.createProject.id)))
dataset4 = nn(chroma.create_or_get_dataset("dontdupe", int(project.createProject.id)))

embedding_set = nn(chroma.create_embedding_set(int(dataset1.createDataset.id)))

slice1 = nn(chroma.create_slice("favorites", int(dataset1.createDataset.id)))
slice2 = nn(chroma.create_slice("bad labels", int(dataset1.createDataset.id)))

label = nn(chroma.create_label('{"asdf":"1234"}'))
resource = nn(chroma.create_resource('file://123.png'))
datapoint = nn(chroma.create_datapoint(int(dataset1.createDataset.id), int(resource.createResource.id), int(label.createLabel.id)))
tag = nn(chroma.create_tag("im a tag!")) # how to attach this to datapoint?

create_datapoint_set = nn(chroma.create_datapoint_set(1, '{"asdf":"1234"}', 'file://123.png'))
append_tag = nn(chroma.append_tag_to_datapoint_mutation(int(tag.createTag.id), int(datapoint.createDatapoint.id)))
remove_tag = nn(chroma.remove_tag_to_datapoint_mutation(int(tag.createTag.id), int(datapoint.createDatapoint.id)))
append_tag = nn(chroma.append_tag_to_datapoint_mutation(int(tag.createTag.id), int(datapoint.createDatapoint.id)))

# add how to associate a tag with a datapoint

# ML stuff
mlarch1 = nn(chroma.create_model_architecture("yolov3", int(project.createProject.id)))
trainedmodel1 = nn(chroma.create_trained_model(int(mlarch1.createModelArchitecture.id)))
layerset1 = nn(chroma.create_layer_set(int(trainedmodel1.createTrainedModel.id)))
layerset2 = nn(chroma.create_layer_set(int(trainedmodel1.createTrainedModel.id)))
layer1 = nn(chroma.create_layer(int(layerset1.createLayerSet.id)))
layer2 = nn(chroma.create_layer(int(layerset1.createLayerSet.id)))
layer3 = nn(chroma.create_layer(int(layerset1.createLayerSet.id)))

create_project_dedupe = nn(chroma.create_or_get_project("one project one project"))

datapointembeddingset = nn(chroma.create_datapoint_embedding_set(int(dataset1.createDataset.id), '{"asdf":"1234"}', 'file://123.png', '[022,992,002]'))
datapointembeddingset = nn(chroma.create_batch_datapoint_embedding_set(int(dataset1.createDataset.id), '{"asdf":"1234"}', 'file://123.png', '[022,992,002]'))
print("seeded database")
