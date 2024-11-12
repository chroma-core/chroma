from typing import Any, Dict, List, Optional, cast
from hypothesis import given, settings, HealthCheck
import pytest
from chromadb.api import ClientAPI
from chromadb.test.property import invariants
from chromadb.api.types import (
    Document,
    Embedding,
    Embeddings,
    GetResult,
    IDs,
    Metadata,
    Metadatas,
    Where,
    WhereDocument,
)
from chromadb.test.conftest import reset, NOT_CLUSTER_ONLY
import chromadb.test.property.strategies as strategies
import hypothesis.strategies as st
import logging
import random
import re
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase
import numpy as np


def _filter_where_clause(clause: Where, metadata: Optional[Metadata]) -> bool:
    """Return true if the where clause is true for the given metadata map"""
    metadata = metadata or dict()
    key, expr = list(clause.items())[0]

    # Handle the shorthand for equal: {key: val} where val is a simple value
    if (
        isinstance(expr, str)
        or isinstance(expr, bool)
        or isinstance(expr, int)
        or isinstance(expr, float)
    ):
        return _filter_where_clause({key: {"$eq": expr}}, metadata)  # type: ignore[dict-item]

    # expr is a list of clauses
    if key == "$and":
        assert isinstance(expr, list)
        return all(_filter_where_clause(clause, metadata) for clause in expr)

    if key == "$or":
        assert isinstance(expr, list)
        return any(_filter_where_clause(clause, metadata) for clause in expr)

    # expr is an operator expression
    assert isinstance(expr, dict)
    op, val = list(expr.items())[0]
    assert isinstance(metadata, dict)
    if op == "$eq":
        return key in metadata and metadata[key] == val
    elif op == "$ne":
        return key not in metadata or metadata[key] != val
    elif op == "$in":
        return key in metadata and metadata[key] in val  # type: ignore[operator]
    elif op == "$nin":
        return key not in metadata or metadata[key] not in val  # type: ignore[operator]

    # The following conditions only make sense for numeric values
    assert (
        key not in metadata
        or isinstance(metadata[key], int)
        or isinstance(metadata[key], float)
    )
    assert isinstance(val, int) or isinstance(val, float)
    if op == "$gt":
        return key in metadata and metadata[key] > val
    elif op == "$gte":
        return key in metadata and metadata[key] >= val
    elif op == "$lt":
        return key in metadata and metadata[key] < val
    elif op == "$lte":
        return key in metadata and metadata[key] <= val
    else:
        raise ValueError("Unknown operator: {}".format(key))


def _filter_where_doc_clause(clause: WhereDocument, doc: Document) -> bool:
    key, expr = list(clause.items())[0]

    if key == "$and":
        assert isinstance(expr, list)
        return all(_filter_where_doc_clause(clause, doc) for clause in expr)
    if key == "$or":
        assert isinstance(expr, list)
        return any(_filter_where_doc_clause(clause, doc) for clause in expr)

    # Simple $contains clause
    assert isinstance(expr, str)
    if key == "$contains":
        if not doc:
            return False
        # SQLite FTS handles % and _ as word boundaries that are ignored so we need to
        # treat them as wildcards
        if "%" in expr or "_" in expr:
            expr = expr.replace("%", ".").replace("_", ".")
            return re.search(expr, doc) is not None
        return expr in doc
    elif key == "$not_contains":
        if not doc:
            return True
        # SQLite FTS handles % and _ as word boundaries that are ignored so we need to
        # treat them as wildcards
        if "%" in expr or "_" in expr:
            expr = expr.replace("%", ".").replace("_", ".")
            return re.search(expr, doc) is None
        return expr not in doc
    else:
        raise ValueError("Unknown operator: {}".format(key))


EMPTY_DICT: Dict[Any, Any] = {}
EMPTY_STRING: str = ""


def _filter_embedding_set(
    record_set: strategies.RecordSet, filter: strategies.Filter
) -> IDs:
    """Return IDs from the embedding set that match the given filter object"""

    normalized_record_set = invariants.wrap_all(record_set)
    ids = set(normalized_record_set["ids"])

    filter_ids = filter["ids"]

    if filter_ids is not None:
        filter_ids = invariants.wrap(filter_ids)
        assert filter_ids is not None
        # If the filter ids is an empty list then we treat that as get all
        if len(filter_ids) != 0:
            ids = ids.intersection(filter_ids)

    for i in range(len(normalized_record_set["ids"])):
        if filter["where"]:
            metadatas: Metadatas
            if isinstance(normalized_record_set["metadatas"], list):
                metadatas = normalized_record_set["metadatas"]  # type: ignore[assignment]
            else:
                metadatas = [EMPTY_DICT] * len(normalized_record_set["ids"])
            filter_where: Where = filter["where"]
            if not _filter_where_clause(filter_where, metadatas[i]):
                ids.discard(normalized_record_set["ids"][i])

        if filter["where_document"]:
            documents = normalized_record_set["documents"] or [EMPTY_STRING] * len(
                normalized_record_set["ids"]
            )
            if not _filter_where_doc_clause(filter["where_document"], documents[i]):
                ids.discard(normalized_record_set["ids"][i])

    return list(ids)


collection_st = st.shared(
    strategies.collections(add_filterable_data=True, with_hnsw_params=True),
    key="coll",
)
recordset_st = st.shared(
    strategies.recordsets(collection_st, max_size=1000), key="recordset"
)


@settings(
    deadline=90000,
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.large_base_example,
        HealthCheck.filter_too_much,
    ],
)  # type: ignore
@given(
    collection=collection_st,
    record_set=recordset_st,
    filters=st.lists(strategies.filters(collection_st, recordset_st), min_size=1),
    should_compact=st.booleans(),
)
def test_filterable_metadata_get(
    caplog,
    client: ClientAPI,
    collection: strategies.Collection,
    record_set,
    filters,
    should_compact: bool,
) -> None:
    caplog.set_level(logging.ERROR)

    reset(client)
    coll = client.create_collection(
        name=collection.name,
        metadata=collection.metadata,  # type: ignore
        embedding_function=collection.embedding_function,
    )

    initial_version = coll.get_model()["version"]

    coll.add(**record_set)

    if not NOT_CLUSTER_ONLY:
        # Only wait for compaction if the size of the collection is
        # some minimal size
        if should_compact and len(invariants.wrap(record_set["ids"])) > 10:
            # Wait for the model to be updated
            wait_for_version_increase(client, collection.name, initial_version)  # type: ignore

    for filter in filters:
        result_ids = coll.get(**filter)["ids"]
        expected_ids = _filter_embedding_set(record_set, filter)
        assert sorted(result_ids) == sorted(expected_ids)


@settings(
    deadline=90000,
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.large_base_example,
        HealthCheck.filter_too_much,
    ],
)  # type: ignore
@given(
    collection=collection_st,
    record_set=recordset_st,
    filters=st.lists(strategies.filters(collection_st, recordset_st), min_size=1),
    limit=st.integers(min_value=1, max_value=10),
    offset=st.integers(min_value=0, max_value=10),
    should_compact=st.booleans(),
)
def test_filterable_metadata_get_limit_offset(
    caplog,
    client: ClientAPI,
    collection: strategies.Collection,
    record_set,
    filters,
    limit,
    offset,
    should_compact: bool,
) -> None:
    caplog.set_level(logging.ERROR)

    reset(client)
    coll = client.create_collection(
        name=collection.name,
        metadata=collection.metadata,  # type: ignore
        embedding_function=collection.embedding_function,
    )

    initial_version = coll.get_model()["version"]

    coll.add(**record_set)

    if not NOT_CLUSTER_ONLY:
        # Only wait for compaction if the size of the collection is
        # some minimal size
        if should_compact and len(invariants.wrap(record_set["ids"])) > 10:
            # Wait for the model to be updated
            wait_for_version_increase(client, collection.name, initial_version)  # type: ignore

    for filter in filters:
        # add limit and offset to filter
        filter["limit"] = limit
        filter["offset"] = offset
        result_ids = coll.get(**filter)["ids"]
        expected_ids = _filter_embedding_set(record_set, filter)
        offset_id_order = {
            id: index for index, id in enumerate(coll.get(ids=expected_ids)["ids"])
        }
        assert (
            result_ids
            == sorted(expected_ids, key=lambda id: offset_id_order[id])[
                offset : offset + limit
            ]
        )


@settings(
    deadline=90000,
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.large_base_example,
        HealthCheck.filter_too_much,
    ],
)
@given(
    collection=collection_st,
    record_set=recordset_st,
    filters=st.lists(
        strategies.filters(collection_st, recordset_st, include_all_ids=True),
        min_size=1,
    ),
    should_compact=st.booleans(),
)
def test_filterable_metadata_query(
    caplog: pytest.LogCaptureFixture,
    client: ClientAPI,
    collection: strategies.Collection,
    record_set: strategies.RecordSet,
    filters: List[strategies.Filter],
    should_compact: bool,
) -> None:
    caplog.set_level(logging.ERROR)

    reset(client)
    coll = client.create_collection(
        name=collection.name,
        metadata=collection.metadata,  # type: ignore
        embedding_function=collection.embedding_function,
    )
    initial_version = coll.get_model()["version"]
    normalized_record_set = invariants.wrap_all(record_set)

    coll.add(**record_set)  # type: ignore[arg-type]

    if not NOT_CLUSTER_ONLY:
        # Only wait for compaction if the size of the collection is
        # some minimal size
        if should_compact and len(invariants.wrap(record_set["ids"])) > 10:
            # Wait for the model to be updated
            wait_for_version_increase(client, collection.name, initial_version)  # type: ignore

    total_count = len(normalized_record_set["ids"])
    # Pick a random vector
    random_query: Embedding
    if collection.has_embeddings:
        assert normalized_record_set["embeddings"] is not None
        assert all(isinstance(e, list) for e in normalized_record_set["embeddings"])
        random_query = normalized_record_set["embeddings"][
            random.randint(0, total_count - 1)
        ]
    else:
        assert isinstance(normalized_record_set["documents"], list)
        assert collection.embedding_function is not None
        random_query = collection.embedding_function(
            [normalized_record_set["documents"][random.randint(0, total_count - 1)]]
        )[0]
    for filter in filters:
        result_ids = set(
            coll.query(
                query_embeddings=random_query,
                n_results=total_count,
                where=filter["where"],
                where_document=filter["where_document"],
            )["ids"][0]
        )
        expected_ids = set(
            _filter_embedding_set(
                cast(strategies.RecordSet, normalized_record_set), filter
            )
        )
        assert len(result_ids.intersection(expected_ids)) == len(result_ids)


def test_empty_filter(client: ClientAPI) -> None:
    """Test that a filter where no document matches returns an empty result"""
    reset(client)
    coll = client.create_collection(name="test")

    test_ids: IDs = ["1", "2", "3"]
    test_embeddings: Embeddings = [np.array([1, 1]), np.array([2, 2]), np.array([3, 3])]
    test_query_embedding: Embedding = np.array([1, 2])
    test_query_embeddings: Embeddings = [test_query_embedding, test_query_embedding]

    coll.add(ids=test_ids, embeddings=test_embeddings)

    res = coll.query(
        query_embeddings=test_query_embedding,
        where={"q": {"$eq": 4}},  # type: ignore[dict-item]
        n_results=3,
        include=["embeddings", "distances", "metadatas"],  # type: ignore[list-item]
    )
    assert res["ids"] == [[]]
    if res["embeddings"] is not None:
        assert cast(np.ndarray, res["embeddings"][0]).size == 0  # type: ignore
    assert res["distances"] == [[]]
    assert res["metadatas"] == [[]]
    assert set(res["included"]) == set(["embeddings", "distances", "metadatas"])

    res = coll.query(
        query_embeddings=test_query_embeddings,
        where={"test": "yes"},
        n_results=3,
    )
    assert res["ids"] == [[], []]
    assert res["embeddings"] is None
    assert res["distances"] == [[], []]
    assert res["metadatas"] == [[], []]
    assert set(res["included"]) == set(["metadatas", "documents", "distances"])


def test_boolean_metadata(client: ClientAPI) -> None:
    """Test that metadata with boolean values is correctly filtered"""
    reset(client)
    coll = client.create_collection(name="test")

    test_ids: IDs = ["1", "2", "3"]
    test_embeddings: Embeddings = [np.array([1, 1]), np.array([2, 2]), np.array([3, 3])]
    test_metadatas: Metadatas = [{"test": True}, {"test": False}, {"test": True}]

    coll.add(ids=test_ids, embeddings=test_embeddings, metadatas=test_metadatas)

    res = coll.get(where={"test": True})

    assert res["ids"] == ["1", "3"]


def test_get_empty(client: ClientAPI) -> None:
    """Tests that calling get() with empty filters returns nothing"""

    reset(client)
    coll = client.create_collection(name="test")

    test_ids: IDs = ["1", "2", "3"]
    test_embeddings: Embeddings = [np.array([1, 1]), np.array([2, 2]), np.array([3, 3])]
    test_metadatas: Metadatas = [{"test": 10}, {"test": 20}, {"test": 30}]

    def check_empty_res(res: GetResult) -> None:
        assert len(res["ids"]) == 0
        assert res["embeddings"] is not None
        assert len(res["embeddings"]) == 0
        assert res["documents"] is not None
        assert len(res["documents"]) == 0
        assert res["metadatas"] is not None

    coll.add(ids=test_ids, embeddings=test_embeddings, metadatas=test_metadatas)

    res = coll.get(ids=["nope"], include=["embeddings", "metadatas", "documents"])  # type: ignore[list-item]
    check_empty_res(res)
    res = coll.get(
        include=["embeddings", "metadatas", "documents"], where={"test": 100}  # type: ignore[list-item]
    )
    check_empty_res(res)
