import chroma
from chroma.config import Settings
chroma_api = chroma.init()

chroma_api.add_training(
    embedding=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4]], 
    input_uri=["/images/3.png", "/images/5.png"], 
    inference_class=["bicycle", "car"],
	label_class=["bicycle", "car"],
	model_space="sample_space"
)
chroma_api.add_production(
    embedding=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4]], 
    input_uri=["/images/3.png", "/images/5.png"], 
    inference_class=["bicycle", "car"],
	model_space="sample_space"
)
print(chroma_api.count(model_space="sample_space"))


# chroma_api.process(training_dataset_name="training", unlabeled_dataset_name="production", model_space="default_scope")
# chroma_api.get_results()
# chroma_api.process(training_dataset_name="training", unlabeled_dataset_name="production", model_space="sample_space")
# results = chroma_api.get_results(dataset_name="production", n_results=2)
print(chroma_api.count(model_space="sample_space"))

# throws
# return sqrt(add.reduce(s, axis=axis, keepdims=keepdims))
# numpy.AxisError: axis 1 is out of bounds for array of dimension 1