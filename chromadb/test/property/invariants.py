import math
from chromadb.test.property.strategies import RecordSet
from typing import Callable, Optional, Union, List, TypeVar
from typing_extensions import Literal
import numpy as np
from chromadb.api import types
from chromadb.api.models.Collection import Collection
from hypothesis import note
from hypothesis.errors import InvalidArgument

T = TypeVar("T")


def maybe_wrap(value: Union[T, List[T]]) -> Union[None, List[T]]:
    """Wrap a value in a list if it is not a list"""
    if value is None:
        return None
    elif isinstance(value, List):
        return value
    else:
        return [value]


def wrap_all(embeddings: RecordSet) -> RecordSet:
    """Ensure that an embedding set has lists for all its values"""

    if embeddings["embeddings"] is None:
        embedding_list = None
    elif isinstance(embeddings["embeddings"], list):
        if len(embeddings["embeddings"]) > 0:
            if isinstance(embeddings["embeddings"][0], list):
                embedding_list = embeddings["embeddings"]
            else:
                embedding_list = [embeddings["embeddings"]]
        else:
            embedding_list = []
    else:
        raise InvalidArgument("embeddings must be a list, list of lists, or None")

    return {
        "ids": maybe_wrap(embeddings["ids"]),  # type: ignore
        "documents": maybe_wrap(embeddings["documents"]),  # type: ignore
        "metadatas": maybe_wrap(embeddings["metadatas"]),  # type: ignore
        "embeddings": embedding_list,
    }


def count(collection: Collection, embeddings: RecordSet):
    """The given collection count is equal to the number of embeddings"""
    count = collection.count()
    embeddings = wrap_all(embeddings)
    assert count == len(embeddings["ids"])


def _field_matches(
    collection: Collection,
    embeddings: RecordSet,
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
        # Since an RecordSet is the user input, we need to convert the documents to
        # a List since thats what the API returns -> none per entry
        expected_field = [None] * len(embeddings["ids"])
    assert actual_field == expected_field


def ids_match(collection: Collection, embeddings: RecordSet):
    """The actual embedding ids is equal to the expected ids"""
    embeddings = wrap_all(embeddings)
    actual_ids = collection.get(ids=embeddings["ids"], include=[])["ids"]
    # The test_out_of_order_ids test fails because of this in test_add.py
    # Here we sort the ids to match the input order
    embedding_id_to_index = {id: i for i, id in enumerate(embeddings["ids"])}
    actual_ids = sorted(actual_ids, key=lambda id: embedding_id_to_index[id])
    assert actual_ids == embeddings["ids"]


def metadatas_match(collection: Collection, embeddings: RecordSet):
    """The actual embedding metadata is equal to the expected metadata"""
    embeddings = wrap_all(embeddings)
    _field_matches(collection, embeddings, "metadatas")


def documents_match(collection: Collection, embeddings: RecordSet):
    """The actual embedding documents is equal to the expected documents"""
    embeddings = wrap_all(embeddings)
    _field_matches(collection, embeddings, "documents")


def no_duplicates(collection: Collection):
    ids = collection.get()["ids"]
    assert len(ids) == len(set(ids))


# These match what the spec of hnswlib is
# This epsilon is used to prevent division by zero and the value is the same
# https://github.com/nmslib/hnswlib/blob/359b2ba87358224963986f709e593d799064ace6/python_bindings/bindings.cpp#L238
NORM_EPS = 1e-30
distance_functions = {
    "l2": lambda x, y: np.linalg.norm(x - y) ** 2,
    "cosine": lambda x, y: 1
    - np.dot(x, y) / ((np.linalg.norm(x) + NORM_EPS) * (np.linalg.norm(y) + NORM_EPS)),
    "ip": lambda x, y: 1 - np.dot(x, y),
}


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
    record_set: RecordSet,
    n_results: int = 1,
    min_recall: float = 0.99,
    embedding_function: Optional[types.EmbeddingFunction] = None,
):
    """Validate that the API performs nearest_neighbor searches correctly"""
    record_set = wrap_all(record_set)

    if len(record_set["ids"]) == 0:
        return  # nothing to test here

    embeddings = record_set["embeddings"]
    have_embeddings = embeddings is not None and len(embeddings) > 0
    if not have_embeddings:
        assert embedding_function is not None
        assert record_set["documents"] is not None
        # Compute the embeddings for the documents
        embeddings = embedding_function(record_set["documents"])

    # l2 is the default distance function
    distance_function = distance_functions["l2"]
    accuracy_threshold = 1e-6
    if "hnsw:space" in collection.metadata:
        space = collection.metadata["hnsw:space"]
        # TODO: ip and cosine are numerically unstable in HNSW.
        # The higher the dimensionality, the more noise is introduced, since each float element
        # of the vector has noise added, which is then subsequently included in all normalization calculations.
        # This means that higher dimensions will have more noise, and thus more error.
        dim = len(embeddings[0])
        accuracy_threshold = accuracy_threshold * math.pow(10, int(math.log10(dim)))

        if space == "cosine":
            distance_function = distance_functions["cosine"]

        if space == "ip":
            distance_function = distance_functions["ip"]

    # Perform exact distance computation
    indices, distances = _exact_distances(
        embeddings, embeddings, distance_fn=distance_function
    )

    query_results = collection.query(
        query_embeddings=record_set["embeddings"],
        query_texts=record_set["documents"] if not have_embeddings else None,
        n_results=n_results,
        include=["embeddings", "documents", "metadatas", "distances"],
    )

    # Dict of ids to indices
    id_to_index = {id: i for i, id in enumerate(record_set["ids"])}
    missing = 0
    for i, (indices_i, distances_i) in enumerate(zip(indices, distances)):
        expected_ids = np.array(record_set["ids"])[indices_i[:n_results]]
        missing += len(set(expected_ids) - set(query_results["ids"][i]))

        # For each id in the query results, find the index in the embeddings set
        # and assert that the embeddings are the same
        for j, id in enumerate(query_results["ids"][i]):
            # This may be because the true nth nearest neighbor didn't get returned by the ANN query
            unexpected_id = id not in expected_ids
            index = id_to_index[id]

            correct_distance = np.allclose(
                distances_i[index],
                query_results["distances"][i][j],
                atol=accuracy_threshold,
            )
            if unexpected_id:
                # If the ID is unexpcted, but the distance is correct, then we
                # have a duplicate in the data. In this case, we should not reduce recall.
                if correct_distance:
                    missing -= 1
                else:
                    continue
            else:
                assert correct_distance

            assert np.allclose(embeddings[index], query_results["embeddings"][i][j])
            if record_set["documents"] is not None:
                assert (
                    record_set["documents"][index] == query_results["documents"][i][j]
                )
            if record_set["metadatas"] is not None:
                assert (
                    record_set["metadatas"][index] == query_results["metadatas"][i][j]
                )

    size = len(record_set["ids"])
    recall = (size - missing) / size

    try:
        note(
            f"recall: {recall}, missing {missing} out of {size}, accuracy threshold {accuracy_threshold}"
        )
    except InvalidArgument:
        pass  # it's ok if we're running outside hypothesis

    assert recall >= min_recall

    # Ensure that the query results are sorted by distance
    for distance_result in query_results["distances"]:
        assert np.allclose(np.sort(distance_result), distance_result)
