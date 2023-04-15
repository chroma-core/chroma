import pytest
import logging
from hypothesis import given, assume, settings, note
import hypothesis.strategies as st
from typing import List, Set
import chromadb
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from chromadb.test.configurations import configurations
import chromadb.test.property.strategies as strategies
import numpy as np
import numpy
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    rule,
    initialize,
    precondition,
    consumes,
    run_state_machine_as_test,
    multiple,
    invariant,
)
from collections import defaultdict
import time
import chromadb.test.property.invariants as invariants


traces = defaultdict(lambda: 0)


def trace(key):
    global traces
    traces[key] += 1


def print_traces():
    global traces
    for key, value in traces.items():
        print(f"{key}: {value}")


@pytest.fixture(scope="module", params=configurations())
def api(request):
    configuration = request.param
    return chromadb.Client(configuration)


class EmbeddingStateMachine(RuleBasedStateMachine):

    embeddings: strategies.EmbeddingSet
    collection: Collection

    embedding_ids: Bundle = Bundle("embedding_ids")

    def __init__(self, api):
        super().__init__()
        self.api = chromadb.Client(configurations()[0])

    @initialize(
        collection=strategies.collections(),
        dtype=st.shared(st.sampled_from(strategies.float_types), key="dtype"),
        dimension=st.shared(st.integers(min_value=2, max_value=2048), key="dimension"),
    )
    def initialize(self, collection, dtype, dimension):
        self.api.reset()
        self.dtype = dtype
        self.dimension = dimension
        self.collection = self.api.create_collection(**collection)
        global init_count
        trace("init")
        self.embeddings = {"ids": [], "embeddings": [], "metadatas": [], "documents": []}

    @rule(
        target=embedding_ids,
        embedding_set=strategies.embedding_set(
            dtype_st=st.shared(st.sampled_from(strategies.float_types), key="dtype"),
            dimension_st=st.shared(st.integers(min_value=2, max_value=2048), key="dimension"),
        ),
    )
    def add_embeddings(self, embedding_set):
        trace("add_embeddings")
        if len(self.embeddings["ids"]) > 0:
            trace("add_more_embeddings")

        if len(set(self.embeddings["ids"]).intersection(set(embedding_set["ids"]))) > 0:
            trace("found_dup_ids")

        self.collection.add(**embedding_set)
        self._add_embeddings(embedding_set)

        return multiple(*embedding_set["ids"])

    @rule(ids=st.lists(consumes(embedding_ids), min_size=1, max_size=50))
    def delete_by_ids(self, ids):
        trace("remove embeddings")

        indices_to_remove = set()
        for i in range(len(self.embeddings["ids"])):
            if self.embeddings["ids"][i] in ids:
                indices_to_remove.add(i)

        self.collection.delete(ids=ids)
        self._remove_embeddings(indices_to_remove)

    @invariant()
    def count(self):
        assert self.collection.count() == len(self.embeddings["ids"])

    @invariant()
    def ann_accuracy(self):
        invariants.ann_accuracy(
            collection=self.collection, embeddings=self.embeddings, min_recall=0.95
        )

    def _add_embeddings(self, embeddings: strategies.EmbeddingSet):
        self.embeddings["ids"].extend(embeddings["ids"])
        self.embeddings["embeddings"].extend(embeddings["embeddings"])  # type: ignore

        if "metadatas" in embeddings and embeddings["metadatas"] is not None:
            metadatas = embeddings["metadatas"]
        else:
            metadatas = [None] * len(embeddings["ids"])

        if "documents" in embeddings and embeddings["documents"] is not None:
            documents = embeddings["documents"]
        else:
            documents = [None] * len(embeddings["ids"])

        self.embeddings["metadatas"].extend(metadatas)  # type: ignore
        self.embeddings["documents"].extend(documents)  # type: ignore

    def _remove_embeddings(self, indices_to_remove: Set[int]):

        indices_list = list(indices_to_remove)
        indices_list.sort(reverse=True)

        for i in indices_list:
            del self.embeddings["ids"][i]
            del self.embeddings["embeddings"][i]  # type: ignore
            del self.embeddings["metadatas"][i]  # type: ignore
            del self.embeddings["documents"][i]  # type: ignore


def test_embeddings_fn(caplog, api):
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: EmbeddingStateMachine(api))
    print_traces()


def test_failure_scenario(caplog, api):
    state = EmbeddingStateMachine(api)
    state.initialize(collection={"name": "A00", "metadata": None}, dtype=numpy.float16, dimension=2)
    state.ann_accuracy()
    state.count()
    (v1,) = state.add_embeddings(
        embedding_set={
            "ids": [""],
            "embeddings": [[0.09765625, 0.430419921875]],
            "metadatas": [{}],
            "documents": ["0"],
        }
    )
    state.ann_accuracy()

    state.count()
    (v2,) = state.add_embeddings(
        embedding_set={
            "ids": [v1],
            "embeddings": [[0.20556640625, 0.08978271484375]],
            "metadatas": [{}],
            "documents": None,
        }
    )
    state.count()
    state.delete_by_ids(ids=[v1])
    state.ann_accuracy()
    state.teardown()


def test_multi_add(api):
    coll = api.create_collection(name="foo")
    coll.add(ids=["a"], embeddings=[[0.0]])
    assert coll.count() == 1
    coll.add(ids=["a"], embeddings=[[0.5]])
    assert coll.count() == 2

    results = coll.query(query_embeddings=[[0.0]], n_results=2)
    assert results["ids"] == [["a", "a"]]

    coll.delete(ids=["a"])
    assert coll.count() == 0


def test_escape_chars_in_ids(api):
    id = "\x1f"
    coll = api.create_collection(name="foo")
    coll.add(ids=[id], embeddings=[[0.0]])
    assert coll.count() == 1
    coll.delete(ids=[id])
    assert coll.count() == 0
