from typing import Optional, Tuple

import hdbscan
import numpy as np
import pandas as pd
from chroma.db.index.hnswlib import Hnswlib


def activation_uncertainty(training_data: pd.DataFrame, inference_data: pd.DataFrame) -> np.ndarray:
    """Calculate the activation uncertainty of the inference data, given the class-wise activation uncertainties in the training data.
    Args:
        training_data (DataFrame): training data as a Pandas DataFrame.
        inference_data (DataFrame): inference data for which to calculate the activation uncertainty as a Pandas DataFrame.
    Returns:
        numpy NDArray: Class-wise activation uncertainty for each inference datapoint.
    """

    activation_uncertainty = np.empty(len(inference_data), dtype=np.float32)
    for class_name in inference_data["inference_class"].unique():
        training_data_class = training_data[training_data["label_class"] == class_name]
        training_data_class_activation = np.linalg.norm(
            training_data_class["embedding"].tolist(), axis=1
        )
        
        inference_data_class = inference_data[inference_data["inference_class"] == class_name]
        inference_data_class_activation = np.linalg.norm(
            inference_data_class["embedding"].tolist(), axis=1
        )

        # Compute the percentile of each training data class activation
        training_sorted_indices = np.argsort(training_data_class_activation)
        training_percentiles = training_sorted_indices / len(training_data_class_activation)

        # Compute the approximate percentile of each inference activation
        # TODO: Do interpolation to get a more accurate percentile
        inference_positions = np.searchsorted(
            training_data_class_activation, inference_data_class_activation
        )
        # Compensate for being past the last element
        inference_positions[inference_positions == len(training_data_class_activation)] -= 1
        
        inference_parcentiles = training_percentiles[inference_positions]

        activation_uncertainty[
            inference_data["inference_class"] == class_name
        ] = inference_parcentiles

    return activation_uncertainty


def boundary_uncertainty(
    training_data: pd.DataFrame,
    inference_data: pd.DataFrame,
    ann_index: Hnswlib,
    n_neighbors: Optional[int] = 100,
    model_space: Optional[str] = "default_scope",
) -> np.ndarray:
    """Calculate the boundary uncertainty of the inference data, the training data, and an ANN index containing both.
    Args:
        training_data (DataFrame): training data as a Pandas DataFrame.
        inference_data (DataFrame): inference data for which to calculate the uncertainty as a Pandas DataFrame.
        ann_index (Hnswlib): Approximate nearest neighbour index of the training and inference data.
        n_neighbors (int): (Optional) Number of nearest neighbours to consider. Defaults to 10.
        model_space(str): (Optional) The model space to use for the ANN index. Defaults to "default_scope".
    Returns:
        numpy NDArray: Boundary uncertainty for each inference datapoint.
    """

    # Get ids for embeddings in the training dataset
    training_ids = training_data["uuid"].tolist()
    training_id_to_idx = {training_ids[i].hex: i for i in range(len(training_ids))}

    neighbor_ids, distances = ann_index.get_nearest_neighbors(
        model_space=model_space,
        query=inference_data["embedding"].tolist(),
        k=n_neighbors,
        uuids=training_ids,
    )
    neighbor_ids = np.array(neighbor_ids)

    flat_idxs = [training_id_to_idx[n_id.hex] for n_id in neighbor_ids.reshape(-1)]
    neighbor_categories = (
        training_data["label_class"].iloc[flat_idxs].to_numpy().reshape(neighbor_ids.shape)
    )

    validation_inference_categories = inference_data["inference_class"].to_numpy()
    matches = neighbor_categories == validation_inference_categories[:, np.newaxis]

    return np.sum(matches, axis=1) / n_neighbors


def class_outliers(
    training_data: pd.DataFrame,
    inference_data: pd.DataFrame,
    ann_index: Hnswlib,
    n_neighbors: Optional[int] = 100,
    model_space: Optional[str] = "default_scope",
) -> Tuple[np.ndarray, np.ndarray]:
    """Calculate the class outlier score of the inference data, the training data, and an ANN index containing both.
    Args:
        training_data (DataFrame): training data as a Pandas DataFrame.
        inference_data (DataFrame): inference data for which to calculate the outlier score as a Pandas DataFrame.
        ann_index (Hnswlib): Approximate nearest neighbour index of the training and inference data.
        n_neighbors (int): (Optional) Number of nearest neighbours to consider. Defaults to 10.
        model_space(str): (Optional) The model space to use for the ANN index. Defaults to "default_scope".
    Returns:
        Tuple[numpy NDArray, numpy NDArray]: Representative class-based outlier percentiles, Difficult class-based outlier percentiles.
    """

    # This does not yet function, raise an exception if this gets called
    # See core_algorithms_examples.ipynb for an explanation. 
    raise NotImplementedError

    representative_outliers = np.empty(len(inference_data), dtype=np.float32)
    difficult_outliers = np.empty(len(inference_data), dtype=np.float32)
    for class_name in inference_data.inference_class.unique():
        training_data_class = training_data[training_data["label_class"] == class_name]
        inference_data_class = inference_data[inference_data["inference_class"] == class_name]

        t_neighbor_ids, t_neighbor_dists = ann_index.get_nearest_neighbors(
            model_space=model_space,
            query=inference_data_class["embedding"].tolist(),
            k=n_neighbors,
            uuids=training_data_class["uuid"].tolist(),
        )
        v_neighbor_ids, v_neighbor_dists = ann_index.get_nearest_neighbors(
            model_space=model_space,
            query=inference_data_class["embedding"].tolist(),
            k=n_neighbors,
            uuids=inference_data_class["uuid"].tolist(),
        )

        dist_diff = np.median(t_neighbor_dists, axis=1) - np.median(v_neighbor_dists, axis=1)
        dist_diff_percentiles = np.argsort(dist_diff) / len(dist_diff)

        representative_outliers[
            inference_data["inference_class"] == class_name
        ] = dist_diff_percentiles

        overall_median = np.median(
            np.concatenate((t_neighbor_dists, v_neighbor_dists), axis=1), axis=1
        )
        overall_median_percentiles = np.argsort(overall_median) / len(overall_median)

        difficult_outliers[
            inference_data["inference_class"] == class_name
        ] = overall_median_percentiles

    return representative_outliers, difficult_outliers


def cluster_outliers(
    training_data: pd.DataFrame,
    inference_data: pd.DataFrame,
    training_subsample: Optional[int] = 10,
    min_cluster_size: Optional[int] = 500,
    min_samples: Optional[int] = 500,
    metric: Optional[str] = "euclidean",
) -> Tuple[np.ndarray, np.ndarray]:
    """Calculate the cluster outlier score of the inference data, the training data, and an ANN index containing both.
    Args:
        training_data (DataFrame): training data as a Pandas DataFrame.
        inference_data (DataFrame): inference data for which to calculate the outlier score as a Pandas DataFrame.
        training_subsample (int): (Optional) Subsampling factor for the training data. Defaults to 10.
        min_cluster_size (int): (Optional) Minimum number of points in a cluster. Defaults to 500.
        min_samples (int): (Optional) Number of samples in a neighbiorhood for a point to be considered. Defaults to 500.
        metric (str): (Optional) Distance metric to use. Defaults to 'euclidean'.
    Returns:
        Tuple[numpy NDArray, numpy NDArray]: Representative cluster-based outlier percentiles, Difficult cluster-based outlier percentiles.
    """

    # Cluster the training data
    training_clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size, min_samples=min_samples, metric=metric
    )
    training_clusterer.fit(training_data["embedding"].tolist()[::training_subsample])
    training_clusterer.generate_prediction_data()

    # Cluster the inference data
    inference_clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size, min_samples=min_samples, metric=metric
    )
    inference_clusterer.fit(inference_data["embedding"].tolist())

    # Get approximate cluster prediction probabilities for the inference data in the training clusters
    labels, t_probabilities = hdbscan.approximate_predict(
        training_clusterer, inference_data["embedding"].tolist()
    )

    representative_outliers = t_probabilities - inference_clusterer.probabilities_
    representative_outlier_percentiles = np.argsort(representative_outliers) / len(
        representative_outliers
    )

    # Get the max of the two probabilities for each cluster
    max_probabilities = np.maximum(t_probabilities, inference_clusterer.probabilities_)

    difficult_outlier_percentiles = np.argsort(max_probabilities) / len(max_probabilities)

    return representative_outlier_percentiles, difficult_outlier_percentiles


def random_sample(inference_data: pd.DataFrame, n_samples: Optional[int] = 1000) -> np.ndarray:
    """Select a random sample of the inference data.
    Args:
        inference_data (DataFrame): inference data to sample.
        n_samples (int): (Optional) Number of samples to select. Defaults to 1000.

    Returns:
        numpy NDArray: Indices of the randomly selected samples.
    """

    # Boolean numpy array of the same length as the inference data
    # with True for the randomly selected samples
    sample = np.zeros(len(inference_data), dtype=bool)
    sample[np.random.choice(len(inference_data), n_samples, replace=False)] = True

    return sample
