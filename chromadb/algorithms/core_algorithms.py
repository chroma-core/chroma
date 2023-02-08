from typing import Optional, Tuple

import hdbscan
import numpy as np
import pandas as pd
from chromadb.db.index.hnswlib import Hnswlib


def activation_uncertainty(training_data: pd.DataFrame, unlabeled_data: pd.DataFrame) -> np.ndarray:
    """Calculate the activation uncertainty of the unlabeled data, given the class-wise activation uncertainties in the training data.
    Args:
        training_data (DataFrame): training data as a Pandas DataFrame.
        unlabeled_data (DataFrame): unlabeled data for which to calculate the activation uncertainty as a Pandas DataFrame.
    Returns:
        numpy NDArray: Class-wise activation uncertainty for each unlabeled datapoint.
    """

    activation_uncertainty = np.empty(len(unlabeled_data), dtype=np.float32)
    for class_name in unlabeled_data["inference_class"].unique():
        training_data_class = training_data[training_data["label_class"] == class_name]
        training_data_class_activation = np.linalg.norm(
            training_data_class["embedding"].tolist(), axis=1
        )

        unlabeled_data_class = unlabeled_data[unlabeled_data["inference_class"] == class_name]
        unlabeled_data_class_activation = np.linalg.norm(
            unlabeled_data_class["embedding"].tolist(), axis=1
        )

        # Compute the percentile of each training data class activation
        training_sorted_indices = np.argsort(training_data_class_activation)
        training_percentiles = training_sorted_indices / len(training_data_class_activation)

        # Compute the approximate percentile of each unlabeled activation
        # TODO: Do interpolation to get a more accurate percentile
        unlabeled_positions = np.searchsorted(
            training_data_class_activation, unlabeled_data_class_activation
        )
        # Compensate for being past the last element
        unlabeled_positions[unlabeled_positions == len(training_data_class_activation)] -= 1

        unlabeled_percentiles = training_percentiles[unlabeled_positions]

        activation_uncertainty[
            unlabeled_data["inference_class"] == class_name
        ] = unlabeled_percentiles

    return activation_uncertainty


def boundary_uncertainty(
    training_data: pd.DataFrame,
    unlabeled_data: pd.DataFrame,
    ann_index: Hnswlib,
    n_neighbors: Optional[int] = 100,
    model_space: Optional[str] = "default_scope",
) -> np.ndarray:
    """Calculate the boundary uncertainty of the unlabeled data, using the training data, and an ANN index containing the training data.
    Args:
        training_data (DataFrame): training data as a Pandas DataFrame.
        unlabeled_data (DataFrame): unlabeled data for which to calculate the uncertainty as a Pandas DataFrame.
        ann_index (Hnswlib): Approximate nearest neighbour index containing the training data.
        n_neighbors (int): (Optional) Number of nearest neighbours to consider. Defaults to 10.
        model_space(str): (Optional) The model space to use for the ANN index. Defaults to "default_scope".
    Returns:
        numpy NDArray: Boundary uncertainty for each unlabeled datapoint.
    """

    # Get ids for embeddings in the training dataset
    training_ids = training_data["uuid"].tolist()
    training_id_to_idx = {training_ids[i].hex: i for i in range(len(training_ids))}

    neighbor_ids, distances = ann_index.get_nearest_neighbors(
        model_space=model_space,
        query=unlabeled_data["embedding"].tolist(),
        k=n_neighbors,
    )
    neighbor_ids = np.array(neighbor_ids)

    flat_idxs = [training_id_to_idx[n_id.hex] for n_id in neighbor_ids.reshape(-1)]
    neighbor_categories = (
        training_data["label_class"].iloc[flat_idxs].to_numpy().reshape(neighbor_ids.shape)
    )

    unlabeled_inference_classes = unlabeled_data["inference_class"].to_numpy()
    matches = neighbor_categories == unlabeled_inference_classes[:, np.newaxis]

    return np.sum(matches, axis=1) / n_neighbors


def class_outliers(
    training_data: pd.DataFrame,
    unlabeled_data: pd.DataFrame,
    ann_index: Hnswlib,
    n_neighbors: Optional[int] = 100,
    model_space: Optional[str] = "default_scope",
) -> Tuple[np.ndarray, np.ndarray]:
    """Calculate the class outlier score of the unlabeled data, the training data, and an ANN index containing both.
    Args:
        training_data (DataFrame): training data as a Pandas DataFrame.
        unlabeled_data (DataFrame): unlabeled data for which to calculate the outlier score as a Pandas DataFrame.
        ann_index (Hnswlib): Approximate nearest neighbour index of the training and unlabeled data.
        n_neighbors (int): (Optional) Number of nearest neighbours to consider. Defaults to 10.
        model_space(str): (Optional) The model space to use for the ANN index. Defaults to "default_scope".
    Returns:
        Tuple[numpy NDArray, numpy NDArray]: Representative class-based outlier percentiles, Difficult class-based outlier percentiles.
    """

    # This does not yet function, raise an exception if this gets called
    # See core_algorithms_examples.ipynb for an explanation.
    raise NotImplementedError

    representative_outliers = np.empty(len(unlabeled_data), dtype=np.float32)
    difficult_outliers = np.empty(len(unlabeled_data), dtype=np.float32)
    for class_name in unlabeled_data.inference_class.unique():
        training_data_class = training_data[training_data["label_class"] == class_name]
        unlabeled_data_class = unlabeled_data[unlabeled_data["inference_class"] == class_name]

        t_neighbor_ids, t_neighbor_dists = ann_index.get_nearest_neighbors(
            model_space=model_space,
            query=unlabeled_data_class["embedding"].tolist(),
            k=n_neighbors,
            uuids=training_data_class["uuid"].tolist(),
        )
        v_neighbor_ids, v_neighbor_dists = ann_index.get_nearest_neighbors(
            model_space=model_space,
            query=unlabeled_data_class["embedding"].tolist(),
            k=n_neighbors,
            uuids=unlabeled_data_class["uuid"].tolist(),
        )

        dist_diff = np.median(t_neighbor_dists, axis=1) - np.median(v_neighbor_dists, axis=1)
        dist_diff_percentiles = np.argsort(dist_diff) / len(dist_diff)

        representative_outliers[
            unlabeled_data["inference_class"] == class_name
        ] = dist_diff_percentiles

        overall_median = np.median(
            np.concatenate((t_neighbor_dists, v_neighbor_dists), axis=1), axis=1
        )
        overall_median_percentiles = np.argsort(overall_median) / len(overall_median)

        difficult_outliers[
            unlabeled_data["inference_class"] == class_name
        ] = overall_median_percentiles

    return representative_outliers, difficult_outliers


def cluster_outliers(
    training_data: pd.DataFrame,
    unlabeled_data: pd.DataFrame,
    training_subsample: Optional[int] = 10,
    min_cluster_size: Optional[int] = 500,
    min_samples: Optional[int] = 500,
    metric: Optional[str] = "euclidean",
) -> Tuple[np.ndarray, np.ndarray]:
    """Calculate the cluster outlier score of the unlabeled data, the training data, and an ANN index containing both.
    Args:
        training_data (DataFrame): training data as a Pandas DataFrame.
        unlabeled_data (DataFrame): unlabeled data for which to calculate the outlier score as a Pandas DataFrame.
        training_subsample (int): (Optional) Subsampling factor for the training data. Defaults to 10.
        min_cluster_size (int): (Optional) Minimum number of points in a cluster. Defaults to 500.
        min_samples (int): (Optional) Number of samples in a neighbiorhood for a point to be considered. Defaults to 500.
        metric (str): (Optional) Distance metric to use. Defaults to 'euclidean'.
    Returns:
        Tuple[numpy NDArray, numpy NDArray]: Representative cluster-based outlier percentiles, Difficult cluster-based outlier percentiles, for the unlabeled data.
    """

    # Cluster the training data
    training_clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size, min_samples=min_samples, metric=metric
    )
    training_clusterer.fit(training_data["embedding"].tolist()[::training_subsample])
    training_clusterer.generate_prediction_data()

    # Cluster the unlabeled data
    unlabeled_clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size, min_samples=min_samples, metric=metric
    )
    unlabeled_clusterer.fit(unlabeled_data["embedding"].tolist())

    # Get approximate cluster prediction probabilities for the unlabeled data in the training clusters
    labels, t_probabilities = hdbscan.approximate_predict(
        training_clusterer, unlabeled_data["embedding"].tolist()
    )

    representative_outliers = t_probabilities - unlabeled_clusterer.probabilities_
    representative_outlier_percentiles = np.argsort(representative_outliers) / len(
        representative_outliers
    )

    # Get the max of the two probabilities for each cluster
    max_probabilities = np.maximum(t_probabilities, unlabeled_clusterer.probabilities_)

    difficult_outlier_percentiles = np.argsort(max_probabilities) / len(max_probabilities)

    return representative_outlier_percentiles, difficult_outlier_percentiles


def random_sample(unlabeled_data: pd.DataFrame, n_samples: Optional[int] = 1000) -> np.ndarray:
    """Select a random sample of the unlabeled data.
    Args:
        unlabeled_data (DataFrame): unlabeled data to sample.
        n_samples (int): (Optional) Number of samples to select. Defaults to 1000.

    Returns:
        numpy NDArray: Indices of the randomly selected samples.
    """

    # Boolean numpy array of the same length as the unlabeled data
    # with True for the randomly selected samples
    sample = np.zeros(len(unlabeled_data), dtype=bool)
    sample[np.random.choice(len(unlabeled_data), n_samples, replace=False)] = True

    return sample
