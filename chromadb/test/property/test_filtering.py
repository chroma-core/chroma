import pytest
from hypothesis import given, example, settings, HealthCheck
import chromadb
from chromadb.api import API
from chromadb.test.configurations import configurations
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
import hypothesis.strategies as st
import logging


@pytest.fixture(scope="module", params=configurations())
def api(request):
    configuration = request.param
    return chromadb.Client(configuration)


def _filter_where_clause(clause, mm):
    """Return true if the where clause is true for the given metadata map"""

    key, expr = list(clause.items())[0]

    if isinstance(expr, str) or isinstance(expr, int) or isinstance(expr, float):
        return _filter_where_clause({key: {"$eq": expr}}, mm)

    if key == "$and":
        return all(_filter_where_clause(clause, mm) for clause in expr)
    if key == "$or":
        return any(_filter_where_clause(clause, mm) for clause in expr)

    op = list(expr.keys())[0]
    val = expr[op]

    if op == "$eq":
        return mm.get(key, None) == val
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

def _filter_embedding_set(recordset: strategies.RecordSet,
                          filter: strategies.Filter):
    """Return IDs from the embedding set that match the given filter object"""
    ids = set(recordset["ids"])

    if filter["ids"]:
        ids = ids.intersection(filter["ids"])

    for i in range(len(recordset["ids"])):

        if filter["where"]:
            metadatas = recordset["metadatas"] or [{}] * len(recordset["ids"])
            if not _filter_where_clause(filter["where"], metadatas[i]):
                ids.discard(recordset["ids"][i])

        if filter["where_document"]:
            documents = recordset["documents"] or [""] * len(recordset["ids"])
            if not _filter_where_doc_clause(filter["where_document"],
                                            documents[i]):
                ids.discard(recordset["ids"][i])

    return list(ids)


collection_st = st.shared(strategies.collections(add_filterable_data=True), key="coll")
recordset_st = st.shared(strategies.recordsets(collection_st,
                                                max_size=1000), key="recordset")


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture,
                                 HealthCheck.large_base_example])
@given(collection=collection_st,
       recordset=recordset_st,
       filters=st.lists(strategies.filters(collection_st, recordset_st), min_size=1))
def test_filterable_metadata(caplog, api, collection, recordset, filters):
    caplog.set_level(logging.ERROR)

    api.reset()
    coll = api.create_collection(name=collection.name,
                                 metadata=collection.metadata,
                                 embedding_function=collection.embedding_function)
    coll.add(**recordset)

    invariants.ann_accuracy(coll, recordset)

    for filter in filters:
        result_ids = coll.get(**filter)["ids"]
        expected_ids = _filter_embedding_set(recordset, filter)
        assert sorted(result_ids) == sorted(expected_ids)

