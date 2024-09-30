import gc
import math
from chromadb.config import System
from chromadb.db.base import get_sql
from chromadb.db.impl.sqlite import SqliteDB
from time import sleep
import psutil
from chromadb.test.property.strategies import NormalizedRecordSet, RecordSet
from typing import Callable, Optional, Tuple, Union, List, TypeVar, cast, Any
from typing_extensions import Literal
import numpy as np
import numpy.typing as npt
from chromadb.api import types
from chromadb.api.models.Collection import Collection
from hypothesis import note
from hypothesis.errors import InvalidArgument
from pypika import Table, functions

from chromadb.utils import distance_functions

T = TypeVar("T")


def wrap(value: Union[T, List[T]]) -> List[T]:
    """Wrap a value in a list if it is not a list"""
    if value is None:
        raise InvalidArgument("value cannot be None")
    elif isinstance(value, List):
        return value
    else:
        return [value]


def wrap_all(record_set: RecordSet) -> NormalizedRecordSet:
    """Ensure that an embedding set has lists for all its values"""

    embedding_list: Optional[types.Embeddings]
    if record_set["embeddings"] is None:
        embedding_list = None
    elif isinstance(record_set["embeddings"], list):
        assert record_set["embeddings"] is not None
        if len(record_set["embeddings"]) > 0:
            if all(
                isinstance(embedding, list) for embedding in record_set["embeddings"]
            ):
                embedding_list = cast(types.Embeddings, record_set["embeddings"])
            elif all(
                isinstance(embedding, np.ndarray)
                for embedding in record_set["embeddings"]
            ):
                embedding_list = cast(types.Embeddings, record_set["embeddings"])
            else:
                if all(
                    isinstance(e, (int, float, np.integer, np.floating))
                    for e in record_set["embeddings"]
                ):
                    embedding_list = cast(types.Embeddings, [record_set["embeddings"]])
                else:
                    raise InvalidArgument(
                        "an embedding must be a list of floats or ints"
                    )
        else:
            embedding_list = cast(types.Embeddings, record_set["embeddings"])
    else:
        raise InvalidArgument(
            "embeddings must be a list of lists, a list of numpy arrays, a list of numbers, or None"
        )

    return {
        "ids": wrap(record_set["ids"]),
        "documents": wrap(record_set["documents"])
        if record_set["documents"] is not None
        else None,
        "metadatas": wrap(record_set["metadatas"])
        if record_set["metadatas"] is not None
        else None,
        "embeddings": embedding_list,
    }


def count(collection: Collection, record_set: RecordSet) -> None:
    """The given collection count is equal to the number of embeddings"""
    count = collection.count()
    normalized_record_set = wrap_all(record_set)
    assert count == len(normalized_record_set["ids"])


def _field_matches(
    collection: Collection,
    normalized_record_set: NormalizedRecordSet,
    field_name: Union[
        Literal["documents"], Literal["metadatas"], Literal["embeddings"]
    ],
) -> None:
    """
    The actual embedding field is equal to the expected field
    field_name: one of [documents, metadatas]
    """
    result = collection.get(ids=normalized_record_set["ids"], include=[field_name])  # type: ignore[list-item]
    # The test_out_of_order_ids test fails because of this in test_add.py
    # Here we sort by the ids to match the input order
    embedding_id_to_index = {id: i for i, id in enumerate(normalized_record_set["ids"])}
    actual_field = result[field_name]

    if len(normalized_record_set["ids"]) == 0:
        if field_name == "embeddings":
            assert cast(npt.NDArray[Any], actual_field).size == 0
        else:
            assert actual_field == []
        return

    # This assert should never happen, if we include metadatas/documents it will be
    # [None, None..] if there is no metadata. It will not be just None.
    assert actual_field is not None
    sorted_field = sorted(
        enumerate(actual_field),
        key=lambda index_and_field_value: embedding_id_to_index[
            result["ids"][index_and_field_value[0]]
        ],
    )
    field_values = [field_value for _, field_value in sorted_field]

    expected_field = normalized_record_set[field_name]
    if expected_field is None:
        # Since an RecordSet is the user input, we need to convert the documents to
        # a List since thats what the API returns -> none per entry
        expected_field = [None] * len(normalized_record_set["ids"])  # type: ignore
    if field_name == "embeddings":
        assert np.allclose(np.array(field_values), np.array(expected_field))
    else:
        assert field_values == expected_field


def ids_match(collection: Collection, record_set: RecordSet) -> None:
    """The actual embedding ids is equal to the expected ids"""
    normalized_record_set = wrap_all(record_set)
    actual_ids = collection.get(ids=normalized_record_set["ids"], include=[])["ids"]
    # The test_out_of_order_ids test fails because of this in test_add.py
    # Here we sort the ids to match the input order
    embedding_id_to_index = {id: i for i, id in enumerate(normalized_record_set["ids"])}
    actual_ids = sorted(actual_ids, key=lambda id: embedding_id_to_index[id])
    assert actual_ids == normalized_record_set["ids"]


def metadatas_match(collection: Collection, record_set: RecordSet) -> None:
    """The actual embedding metadata is equal to the expected metadata"""
    normalized_record_set = wrap_all(record_set)
    _field_matches(collection, normalized_record_set, "metadatas")


def documents_match(collection: Collection, record_set: RecordSet) -> None:
    """The actual embedding documents is equal to the expected documents"""
    normalized_record_set = wrap_all(record_set)
    _field_matches(collection, normalized_record_set, "documents")


def embeddings_match(collection: Collection, record_set: RecordSet) -> None:
    """The actual embedding documents is equal to the expected documents"""
    normalized_record_set = wrap_all(record_set)
    _field_matches(collection, normalized_record_set, "embeddings")


def no_duplicates(collection: Collection) -> None:
    ids = collection.get()["ids"]
    assert len(ids) == len(set(ids))


def _exact_distances(
    query: types.Embeddings,
    targets: types.Embeddings,
    distance_fn: Callable[
        [npt.ArrayLike, npt.ArrayLike], float
    ] = distance_functions.l2,
) -> Tuple[List[List[int]], List[List[float]]]:
    """Return the ordered indices and distances from each query to each target"""
    np_query = np.array(query, dtype=np.float32)
    np_targets = np.array(targets, dtype=np.float32)

    # Compute the distance between each query and each target, using the distance function
    distances = np.apply_along_axis(
        lambda query: np.apply_along_axis(distance_fn, 1, np_targets, query),
        1,
        np_query,
    )
    # Sort the distances and return the indices
    return np.argsort(distances).tolist(), distances.tolist()


def fd_not_exceeding_threadpool_size(threadpool_size: int) -> None:
    """
    Checks that the open file descriptors are not exceeding the threadpool size
    works only for SegmentAPI
    """
    current_process = psutil.Process()
    open_files = current_process.open_files()
    max_retries = 5
    retry_count = 0
    # we probably don't need the below but we keep it to avoid flaky tests.
    while (
        len([p.path for p in open_files if "sqlite3" in p.path]) - 1 > threadpool_size
        and retry_count < max_retries
    ):
        gc.collect()  # GC to collect the orphaned TLS objects
        open_files = current_process.open_files()
        retry_count += 1
        sleep(1)
    assert (
        len([p.path for p in open_files if "sqlite3" in p.path]) - 1 <= threadpool_size
    )


def ann_accuracy(
    collection: Collection,
    record_set: RecordSet,
    n_results: int = 1,
    min_recall: float = 0.99,
    embedding_function: Optional[types.EmbeddingFunction] = None,  # type: ignore[type-arg]
    query_indices: Optional[List[int]] = None,
    query_embeddings: Optional[types.Embeddings] = None,
) -> None:
    """Validate that the API performs nearest_neighbor searches correctly"""
    normalized_record_set = wrap_all(record_set)

    if len(normalized_record_set["ids"]) == 0:
        return  # nothing to test here

    embeddings: Optional[types.Embeddings] = normalized_record_set["embeddings"]
    have_embeddings = embeddings is not None and len(embeddings) > 0
    if not have_embeddings:
        assert embedding_function is not None
        assert normalized_record_set["documents"] is not None
        assert isinstance(normalized_record_set["documents"], list)
        # Compute the embeddings for the documents
        embeddings = embedding_function(normalized_record_set["documents"])

    # l2 is the default distance function
    distance_function = distance_functions.l2
    accuracy_threshold = 1e-6
    assert collection.metadata is not None
    assert embeddings is not None
    if "hnsw:space" in collection.metadata:
        space = collection.metadata["hnsw:space"]
        # TODO: ip and cosine are numerically unstable in HNSW.
        # The higher the dimensionality, the more noise is introduced, since each float element
        # of the vector has noise added, which is then subsequently included in all normalization calculations.
        # This means that higher dimensions will have more noise, and thus more error.
        assert all(isinstance(e, (list, np.ndarray)) for e in embeddings)
        dim = len(embeddings[0])
        accuracy_threshold = accuracy_threshold * math.pow(10, int(math.log10(dim)))

        if space == "cosine":
            distance_function = distance_functions.cosine
        if space == "ip":
            distance_function = distance_functions.ip

    # Perform exact distance computation
    if query_embeddings is None:
        query_embeddings = (
            embeddings
            if query_indices is None
            else [embeddings[i] for i in query_indices]
        )
    query_documents = normalized_record_set["documents"]
    if query_indices is not None and query_documents is not None:
        query_documents = [query_documents[i] for i in query_indices]

    indices, distances = _exact_distances(
        query_embeddings, embeddings, distance_fn=distance_function
    )

    query_results = collection.query(
        query_embeddings=query_embeddings if have_embeddings else None,
        query_texts=query_documents if not have_embeddings else None,
        n_results=n_results,
        include=["embeddings", "documents", "metadatas", "distances"],  # type: ignore[list-item]
    )

    _query_results_are_correct_shape(query_results, n_results)

    # Assert fields are not None for type checking
    assert query_results["ids"] is not None
    assert query_results["distances"] is not None
    assert query_results["embeddings"] is not None
    assert query_results["documents"] is not None
    assert query_results["metadatas"] is not None

    # Dict of ids to indices
    id_to_index = {id: i for i, id in enumerate(normalized_record_set["ids"])}
    missing = 0
    for i, (indices_i, distances_i) in enumerate(zip(indices, distances)):
        expected_ids = np.array(normalized_record_set["ids"])[indices_i[:n_results]]
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
            if normalized_record_set["documents"] is not None:
                assert (
                    normalized_record_set["documents"][index]
                    == query_results["documents"][i][j]
                )
            if normalized_record_set["metadatas"] is not None:
                assert (
                    normalized_record_set["metadatas"][index]
                    == query_results["metadatas"][i][j]
                )

    size = len(normalized_record_set["ids"])
    recall = (size - missing) / size

    try:
        note(
            f"# recall: {recall}, missing {missing} out of {size}, accuracy threshold {accuracy_threshold}"
        )
    except InvalidArgument:
        pass  # it's ok if we're running outside hypothesis

    assert recall >= min_recall

    # Ensure that the query results are sorted by distance
    for distance_result in query_results["distances"]:
        assert np.allclose(np.sort(distance_result), distance_result)


def _query_results_are_correct_shape(
    query_results: types.QueryResult, n_results: int
) -> None:
    for result_type in ["distances", "embeddings", "documents", "metadatas"]:
        assert query_results[result_type] is not None  # type: ignore[literal-required]
        assert all(
            len(result) == n_results for result in query_results[result_type]  # type: ignore[literal-required]
        )


def _total_embedding_queue_log_size(sqlite: SqliteDB) -> int:
    t = Table("embeddings_queue")
    q = sqlite.querybuilder().from_(t)

    with sqlite.tx() as cur:
        sql, params = get_sql(
            q.select(functions.Count(t.seq_id)), sqlite.parameter_format()
        )
        result = cur.execute(sql, params)
        return cast(int, result.fetchone()[0])


def log_size_below_max(
    system: System, collections: List[Collection], has_collection_mutated: bool
) -> None:
    sqlite = system.instance(SqliteDB)

    if has_collection_mutated:
        # Must always keep one entry to avoid reusing seq_ids
        assert _total_embedding_queue_log_size(sqlite) >= 1

        # We purge per-collection as the sync_threshold is a per-collection setting
        sync_threshold_sum = sum(
            collection.metadata.get("hnsw:sync_threshold", 1000)
            if collection.metadata is not None
            else 1000
            for collection in collections
        )
        batch_size_sum = sum(
            collection.metadata.get("hnsw:batch_size", 100)
            if collection.metadata is not None
            else 100
            for collection in collections
        )

        # -1 is used because the queue is always at least 1 entry long, so deletion stops before the max ack'ed sequence ID.
        # And if the batch_size != sync_threshold, the queue can have up to batch_size more entries.
        assert (
            _total_embedding_queue_log_size(sqlite) - 1
            <= sync_threshold_sum + batch_size_sum
        )
    else:
        assert _total_embedding_queue_log_size(sqlite) == 0
