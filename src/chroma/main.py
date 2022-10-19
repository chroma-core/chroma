
from chroma import Chroma

if __name__ == "__main__":
    base_metadata = {
        "app":"helloworldapp", 
        "model_version":"1.0.0", 
        "layer":"pool5", 
    }

    chroma = Chroma(base_metadata=base_metadata)

    # first log some training data
    # log_metadata = {
    #     "app":"helloworldapp", 
    #     "model_version":"1.0.0", 
    #     "layer":"pool5", 
    #     "dataset":"training", # eg "training"
    #     "reference_dataset": None
    # }
    # chroma.log(
    #     input_uri="s3://bucket/path/to/input",
    #     inference_data={ "category": "car" },
    #     embedding_data=[1,2,3,4,5,6,7,8,9,10], 
    #     metadata=log_metadata)

    chroma.log_training(
        input_uri="s3://bucket/path/to/input2",
        inference_data={ "category": "car" },
        embedding_data=[10,9,8,7,6,5,4,3,2,1])

    # then log some production data
    # log_prod_metadata = log_metadata.copy()
    # log_prod_metadata['reference_dataset'] = "production"

    # chroma.log(
    #     input_uri="s3://bucket/path/to/input2",
    #     inference_data={ "category": "car" },
    #     embedding_data=[10,9,8,7,6,5,4,3,2,1], 
    #     metadata=log_prod_metadata)

    chroma.log_production(
        input_uri="s3://bucket/path/to/input2",
        inference_data={ "category": "car" },
        embedding_data=[10,9,8,7,6,5,4,3,2,1])

    # then log some triage data
    chroma.log_triage(
        input_uri="s3://bucket/path/to/input2",
        inference_data={ "category": "car" },
        embedding_data=[4,4,4,4,4,4,4,4,4,4,4])

    # now process the data
    # process_metadata = {
    #     "app":"helloworldapp", 
    #     "model_version":"1.0.0", 
    #     "layer":"pool5", 
    # }

    chroma.process()

    # then fetch some results
    # fetch_metadata = {
    #     "app":"helloworldapp", 
    #     "model_version":"1.0.0", 
    #     "layer":"pool5", 
    #     "dataset":"production", # eg "training"
    # }

    chroma.fetch()

