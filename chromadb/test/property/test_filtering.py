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


def _filter(clause, mm):
    """Return true if the where clause is true for the given metadata map"""

    key, expr = list(clause.items())[0]

    if isinstance(expr, str) or isinstance(expr, int) or isinstance(expr, float):
        return _filter({key: {"$eq": expr}}, mm)

    if key == "$and":
        return all(_filter(clause, mm) for clause in expr)
    if key == "$or":
        return any(_filter(clause, mm) for clause in expr)

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


def _filter_embedding_set(es, where_clause):
    """Return IDs from the embedding set that match the where clause"""
    ids = []
    for i in range(len(es["ids"])):
        if _filter(where_clause, es["metadatas"][i]):
            ids.append(es["ids"][i])
    return ids

@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(collection=strategies.collections(),
       es_and_filters=strategies.filterable_embedding_set_with_filters())
def test_filterable_metadata(caplog, api, collection, es_and_filters):
    caplog.set_level(logging.ERROR)
    es, filters, doc_filters = es_and_filters

    api.reset()
    coll = api.create_collection(**collection)
    coll.add(**es)

    invariants.ann_accuracy(coll, es)

    for where_clause in filters:
        result_ids = coll.get(where=where_clause)["ids"]
        expected_ids = _filter_embedding_set(es, where_clause)
        assert sorted(result_ids) == sorted(expected_ids)



def test_failing_case(caplog, api):
    caplog.set_level(logging.ERROR)

    collection = {'name': 'A00', 'metadata': None}

    es = {'ids': ['1', '0'],
          'embeddings': [[0.09765625, 0.430419921875],
                         [0.20556640625, 0.08978271484375]],
          'metadatas': [{}, {'intKey': 0}],
          'documents': ['apple apple', 'apple apple']}

    api.reset()
    coll = api.create_collection(**collection)
    coll.add(**es)

    filters =  [{'intKey': {'$gt': 0}}, {'intKey': {'$ne': 0}}]

    for where_clause in filters:
        result_ids = coll.get(where=where_clause)["ids"]
        expected_ids = _filter_embedding_set(es, where_clause)
        assert sorted(result_ids) == sorted(expected_ids)


