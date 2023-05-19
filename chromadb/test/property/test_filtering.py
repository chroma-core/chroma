from hypothesis import given, settings, HealthCheck
from chromadb.api import API
from chromadb.test.property import invariants
import chromadb.test.property.strategies as strategies
import hypothesis.strategies as st
import logging
import random


def _filter_where_clause(clause, mm):
    """Return true if the where clause is true for the given metadata map"""

    key, expr = list(clause.items())[0]

    # Handle the shorthand for equal: {key: val} where val is a simple value
    if isinstance(expr, str) or isinstance(expr, int) or isinstance(expr, float):
        return _filter_where_clause({key: {"$eq": expr}}, mm)

    if key == "$and":
        return all(_filter_where_clause(clause, mm) for clause in expr)
    if key == "$or":
        return any(_filter_where_clause(clause, mm) for clause in expr)

    op, val = list(expr.items())[0]

    if op == "$eq":
        return key in mm and mm[key] == val
    elif op == "$ne":
        return key in mm and mm[key] != val
    elif op == "$gt":
        return key in mm and mm[key] > val
    elif op == "$gte":
        return key in mm and mm[key] >= val
    elif op == "$lt":
        return key in mm and mm[key] < val
    elif op == "$lte":
        return key in mm and mm[key] <= val
    else:
        raise ValueError("Unknown operator: {}".format(key))


def _filter_where_doc_clause(clause, doc):
    key, expr = list(clause.items())[0]
    if key == "$and":
        return all(_filter_where_doc_clause(clause, doc) for clause in expr)
    elif key == "$or":
        return any(_filter_where_doc_clause(clause, doc) for clause in expr)
    elif key == "$contains":
        return expr in doc
    else:
        raise ValueError("Unknown operator: {}".format(key))


EMPTY_DICT = {}
EMPTY_STRING = ""


def _filter_embedding_set(recordset: strategies.RecordSet, filter: strategies.Filter):
    """Return IDs from the embedding set that match the given filter object"""

    recordset = invariants.wrap_all(recordset)

    ids = set(recordset["ids"])

    filter_ids = filter["ids"]
    if filter_ids is not None:
        filter_ids = invariants.maybe_wrap(filter_ids)
        assert filter_ids is not None
        # If the filter ids is an empty list then we treat that as get all
        if len(filter_ids) != 0:
            ids = ids.intersection(filter_ids)

    for i in range(len(recordset["ids"])):
        if filter["where"]:
            metadatas = recordset["metadatas"] or [EMPTY_DICT] * len(recordset["ids"])
            if not _filter_where_clause(filter["where"], metadatas[i]):
                ids.discard(recordset["ids"][i])

        if filter["where_document"]:
            documents = recordset["documents"] or [EMPTY_STRING] * len(recordset["ids"])
            if not _filter_where_doc_clause(filter["where_document"], documents[i]):
                ids.discard(recordset["ids"][i])

    return list(ids)


collection_st = st.shared(
    strategies.collections(add_filterable_data=True, with_hnsw_params=True),
    key="coll",
)
recordset_st = st.shared(
    strategies.recordsets(collection_st, max_size=1000), key="recordset"
)


@settings(
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.large_base_example,
    ]
)
@given(
    collection=collection_st,
    recordset=recordset_st,
    filters=st.lists(strategies.filters(collection_st, recordset_st), min_size=1),
)
def test_filterable_metadata_get(caplog, api: API, collection, recordset, filters):
    caplog.set_level(logging.ERROR)

    api.reset()
    coll = api.create_collection(
        name=collection.name,
        metadata=collection.metadata,
        embedding_function=collection.embedding_function,
    )
    coll.add(**recordset)

    for filter in filters:
        result_ids = coll.get(**filter)["ids"]
        expected_ids = _filter_embedding_set(recordset, filter)
        assert sorted(result_ids) == sorted(expected_ids)


@settings(
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.large_base_example,
    ]
)
@given(
    collection=collection_st,
    recordset=recordset_st,
    filters=st.lists(
        strategies.filters(collection_st, recordset_st, include_all_ids=True),
        min_size=1,
    ),
)
def test_filterable_metadata_query(
    caplog,
    api: API,
    collection: strategies.Collection,
    recordset: strategies.RecordSet,
    filters,
):
    caplog.set_level(logging.ERROR)

    api.reset()
    coll = api.create_collection(
        name=collection.name,
        metadata=collection.metadata,
        embedding_function=collection.embedding_function,
    )
    coll.add(**recordset)
    recordset = invariants.wrap_all(recordset)
    total_count = len(recordset["ids"])
    # Pick a random vector
    if collection.has_embeddings:
        random_query = recordset["embeddings"][random.randint(0, total_count - 1)]
    else:
        random_query = collection.embedding_function(
            recordset["documents"][random.randint(0, total_count - 1)]
        )
    for filter in filters:
        result_ids = set(
            coll.query(
                query_embeddings=random_query,
                n_results=total_count,
                where=filter["where"],
                where_document=filter["where_document"],
            )["ids"][0]
        )
        expected_ids = set(_filter_embedding_set(recordset, filter))
        assert len(result_ids.intersection(expected_ids)) == len(result_ids)


def test_empty_filter(api):
    """Test that a filter where no document matches returns an empty result"""
    api.reset()
    coll = api.create_collection(name="test")
    coll.add(ids=["1", "2", "3"], embeddings=[[1, 1], [2, 2], [3, 3]])

    res = coll.query(
        query_embeddings=[1, 2],
        where={"q": {"$eq": 4}},
        n_results=3,
        include=["embeddings", "distances", "metadatas"],
    )
    assert res["ids"] == [[]]
    assert res["embeddings"] == [[]]
    assert res["distances"] == [[]]
    assert res["metadatas"] == [[]]

    res = coll.query(
        query_embeddings=[[1, 2], [1, 2]],
        where={"test": "yes"},
        n_results=3,
    )
    assert res["ids"] == [[], []]
    assert res["embeddings"] is None
    assert res["distances"] == [[], []]
    assert res["metadatas"] == [[], []]
