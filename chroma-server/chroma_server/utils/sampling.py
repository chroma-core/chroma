from typing import Optional

from chroma_server.algorithms.core_algorithms import (activation_uncertainty,
                                                      boundary_uncertainty,
                                                      class_outliers,
                                                      cluster_outliers,
                                                      random_sample)
from chroma_server.db.clickhouse import Clickhouse
from chroma_server.index.hnswlib import Hnswlib

def score_and_store(
    training_dataset: str,
    inference_dataset: str,
    n_random_samples,
    db_connection: Clickhouse,
    ann_index: Hnswlib,
    model_space: Optional[str] = "default_scope",
) -> None:

    training_data = db_connection.fetch(
        where_filter={"model_space": model_space, "dataset": training_dataset}
    )
    inference_data = db_connection.fetch(
        where_filter={"model_space": model_space, "dataset": inference_dataset}
    )

    ann_index.load(model_space=model_space)

    activation_uncertainty_scores = activation_uncertainty(
        training_data=training_data, inference_data=inference_data
    )
    boundary_uncertainty_scores = boundary_uncertainty(
        training_data=training_data,
        inference_data=inference_data,
        ann_index=ann_index,
        model_space=model_space,
    )

    representative_class_outlier_scores, difficult_class_outlier_scores = class_outliers(
        training_data=training_data,
        inference_data=inference_data,
        ann_index=ann_index,
        model_space=model_space,
    )
    representative_cluster_outlier_scores, difficult_cluster_outlier_scores = cluster_outliers(
        training_data=training_data, inference_data=inference_data
    )

    random_selection = random_sample(inference_data=inference_data, n_samples=n_random_samples)

    # Only one set of results per model space 
    db_connection.delete_results(model_space=model_space)

    # Results have singular names as arguments so they match DB schema column names
    db_connection.add_results(
        model_space=model_space,
        uuids=inference_data["uuid"].tolist(),
        activation_uncertainty_score=activation_uncertainty_scores,
        boundary_uncertainty_score=boundary_uncertainty_scores,
        representative_class_outlier_score=representative_class_outlier_scores,
        difficult_class_outlier_score=difficult_class_outlier_scores,
        representative_cluster_outlier_score=representative_cluster_outlier_scores,
        difficult_cluster_outlier_score=difficult_cluster_outlier_scores,
        random_selection=random_selection,
    )

    return None
