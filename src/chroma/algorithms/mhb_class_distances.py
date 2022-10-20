import numpy as np
import json
import ast

def class_distances(data):
    ''''
    This is all very subject to change, so essentially just copy and paste from what we had before
    '''

    def unpack_annotations(embeddings):
        annotations = [json.loads(embedding['infer'])["annotations"]for embedding in embeddings]
        annotations = [annotation for annotation_list in annotations for annotation in annotation_list] 
        # Unpack embedding data
        embeddings = [embedding["embedding_data"] for embedding in embeddings]
        embedding_vectors_by_category = {}
        for embedding_annotation_pair in zip(embeddings, annotations):
            data = np.array(embedding_annotation_pair[0])
            category = embedding_annotation_pair[1]['category_id'] 
            if category in embedding_vectors_by_category.keys():
                embedding_vectors_by_category[category] = np.append(
                    embedding_vectors_by_category[category], data[np.newaxis, :], axis=0
                )
            else:
                embedding_vectors_by_category[category] = data[np.newaxis, :]

        return embedding_vectors_by_category

    # Get the training embeddings. This is the set of embeddings belonging to datapoints of the training dataset, and to the first created embedding set.
    object_embedding_vectors_by_category = unpack_annotations(data.to_dict('records'))

    inv_covs_by_category = {}
    means_by_category = {}
    for category, embeddings in object_embedding_vectors_by_category.items():
        print(f"Computing mean and covariance for label category {category}")

        # Compute the mean and inverse covariance for computing MHB distance
        print(f"category: {category} samples: {embeddings.shape[0]}")
        if embeddings.shape[0] < (embeddings.shape[1] + 1):
            print(f"not enough samples for stable covariance in category {category}")
            continue
        cov = np.cov(embeddings.transpose())
        try:
            inv_cov = np.linalg.inv(cov)
        except np.linalg.LinAlgError as err:
            print(f"covariance for category {category} is singular")
            continue
        mean = np.mean(embeddings, axis=0)
        inv_covs_by_category[category] = inv_cov
        means_by_category[category] = mean

    target_datapoints = data.to_dict('records') #+ panda_train_table.to_dict('records')

    output_distances = []

    # Process each datapoint's inferences individually. This is going to be very slow.
    # This is because there is no way to grab the corresponding metadata off the datapoint.
    # We could instead put it on the embedding directly ?
    inference_metadata = {}
    quality_scores = []
    for idx, datapoint in enumerate(target_datapoints):
        inferences = json.loads(datapoint['infer'])["annotations"]
        embeddings = [datapoint["embedding_data"]]

        for i in range(len(inferences)):
            emb_data = embeddings[i]
            category = inferences[i]["category_id"]
            if not category in inv_covs_by_category.keys():
                output_distances.append({"distance": None, "id":  datapoint["id"]})
                continue
            mean = means_by_category[category]
            inv_cov = inv_covs_by_category[category]
            delta = np.array(emb_data) - mean
            squared_mhb = np.sum((delta * np.matmul(inv_cov, delta)), axis=0)
            if squared_mhb < 0:
                print(f"squared distance for category {category} is negative")
                output_distances.append({"distance": None, "id":  datapoint["id"]})
                continue
            distance = np.sqrt(squared_mhb)
            quality_scores.append([distance, datapoint])
            inference_metadata[datapoint["input_uri"]] = distance
            output_distances.append({"id": datapoint["id"], "distance": distance})

        if (len(inferences) == 0):
            raise Exception("No inferences found for datapoint")
    
    return output_distances
