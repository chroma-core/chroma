from typing import Callable, Literal, Sequence, Union, cast
from chromadb.test.property.strategies import EmbeddingSet
import numpy as np
from chromadb.api import API, types
from chromadb.api.models.Collection import Collection
from hypothesis import note
from hypothesis.errors import InvalidArgument


def count(api: API, collection_name: str, expected_count: int):
    """The given collection count is equal to the number of embeddings"""
    collection = api.get_collection(collection_name, embedding_function=lambda x: None)
    count = collection.count()
    assert count == expected_count


def _field_matches(
    collection: Collection,
    embeddings: EmbeddingSet,
    field_name: Union[Literal["documents"], Literal["metadatas"]],
):
    """
    The actual embedding field is equal to the expected field
    field_name: one of [documents, metadatas]
    """
    result = collection.get(ids=embeddings["ids"], include=[field_name])
    # The test_out_of_order_ids test fails because of this in test_add.py
    # Here we sort by the ids to match the input order
    embedding_id_to_index = {id: i for i, id in enumerate(embeddings["ids"])}
    actual_field = result[field_name]
    # This assert should never happen, if we include metadatas/documents it will be
    # [None, None..] if there is no metadata. It will not be just None.
    assert actual_field is not None
    actual_field = sorted(
        enumerate(actual_field),
        key=lambda index_and_field_value: embedding_id_to_index[
            result["ids"][index_and_field_value[0]]
        ],
    )
    actual_field = [field_value for _, field_value in actual_field]

    expected_field = embeddings[field_name]
    if expected_field is None:
        # Since an EmbeddingSet is the user input, we need to convert the documents to
        # a List since thats what the API returns -> none per entry
        expected_field = [None] * len(embeddings["ids"])
    assert actual_field == expected_field


def ids_match(collection: Collection, embeddings: EmbeddingSet):
    """The actual embedding ids is equal to the expected ids"""
    actual_ids = collection.get(ids=embeddings["ids"], include=[])["ids"]
    # The test_out_of_order_ids test fails because of this in test_add.py
    # Here we sort the ids to match the input order
    embedding_id_to_index = {id: i for i, id in enumerate(embeddings["ids"])}
    actual_ids = sorted(actual_ids, key=lambda id: embedding_id_to_index[id])
    assert actual_ids == embeddings["ids"]


def metadatas_match(collection: Collection, embeddings: EmbeddingSet):
    """The actual embedding metadata is equal to the expected metadata"""
    _field_matches(collection, embeddings, "metadatas")


def documents_match(collection: Collection, embeddings: EmbeddingSet):
    """The actual embedding documents is equal to the expected documents"""
    _field_matches(collection, embeddings, "documents")


def no_duplicates(collection: Collection):
    ids = collection.get()["ids"]
    assert len(ids) == len(set(ids))


def _exact_distances(
    query: types.Embeddings,
    targets: types.Embeddings,
    distance_fn: Callable = lambda x, y: np.linalg.norm(x - y) ** 2,
):
    """Return the ordered indices and distances from each query to each target"""
    np_query = np.array(query)
    np_targets = np.array(targets)

    # Compute the distance between each query and each target, using the distance function
    distances = np.apply_along_axis(
        lambda query: np.apply_along_axis(distance_fn, 1, np_targets, query),
        1,
        np_query,
    )
    # Sort the distances and return the indices
    return np.argsort(distances), distances


def ann_accuracy(
    collection: Collection,
    embeddings: EmbeddingSet,
    n_results: int = 1,
    min_recall: float = 0.99,
):
    """Validate that the API performs nearest_neighbor searches correctly"""

    if len(embeddings["ids"]) == 0:
        return  # nothing to test here

    # TODO Remove once we support querying by documents in tests
    if embeddings["embeddings"] is None:
        # If we don't have embeddings, we can't do an ANN search
        return

    # Perform exact distance computation
    indices, distances = _exact_distances(
        embeddings["embeddings"], embeddings["embeddings"]
    )

    query_results = collection.query(
        query_embeddings=embeddings["embeddings"],
        query_texts=embeddings["documents"]
        if embeddings["embeddings"] is None
        else None,
        n_results=n_results,
        include=["embeddings", "documents", "metadatas", "distances"],
    )

    # Dict of ids to indices
    id_to_index = {id: i for i, id in enumerate(embeddings["ids"])}
    missing = 0
    for i, (indices_i, distances_i) in enumerate(zip(indices, distances)):
        expected_ids = np.array(embeddings["ids"])[indices_i[:n_results]]
        missing += len(set(expected_ids) - set(query_results["ids"][i]))

        # For each id in the query results, find the index in the embeddings set
        # and assert that the embeddings are the same
        for j, id in enumerate(query_results["ids"][i]):
            # This may be because the true nth nearest neighbor didn't get returned by the ANN query
            if id not in expected_ids:
                continue
            index = id_to_index[id]
            assert np.allclose(distances_i[index], query_results["distances"][i][j])
            assert np.allclose(
                embeddings["embeddings"][index], query_results["embeddings"][i][j]
            )
            if embeddings["documents"] is not None:
                assert (
                    embeddings["documents"][index] == query_results["documents"][i][j]
                )
            if embeddings["metadatas"] is not None:
                assert (
                    embeddings["metadatas"][index] == query_results["metadatas"][i][j]
                )

    size = len(embeddings["ids"])
    recall = (size - missing) / size

    try:
        note(f"recall: {recall}, missing {missing} out of {size}")
    except InvalidArgument:
        pass  # it's ok if we're running outside hypothesis

    assert recall >= min_recall
