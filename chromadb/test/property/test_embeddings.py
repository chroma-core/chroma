import pytest
import logging
from hypothesis import given, assume, settings, note
import hypothesis.strategies as st
from typing import List, Set, TypedDict, Sequence
import chromadb
import chromadb.errors as errors
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from chromadb.test.configurations import configurations
import chromadb.api.types as types
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


dtype_shared_st = st.shared(st.sampled_from(strategies.float_types), key="dtype")
dimension_shared_st = st.shared(st.integers(min_value=2, max_value=2048), key="dimension")


class EmbeddingStateMachine(RuleBasedStateMachine):

    collection: Collection
    embedding_ids: Bundle = Bundle("embedding_ids")

    def __init__(self, api):
        super().__init__()
        self.api = api

    @initialize(
        collection=strategies.collections(),
        dtype=dtype_shared_st,
        dimension=dimension_shared_st,
    )
    def initialize(self, collection, dtype, dimension):
        self.api.reset()
        self.dtype = dtype
        self.dimension = dimension
        self.collection = self.api.create_collection(**collection)
        trace("init")
        self.embeddings = {"ids": [], "embeddings": [], "metadatas": [], "documents": []}

    @rule(
        target=embedding_ids,
        embedding_set=strategies.embedding_set(
            dtype_st=dtype_shared_st, dimension_st=dimension_shared_st
        ),
    )
    def add_embeddings(self, embedding_set):
        trace("add_embeddings")
        if len(self.embeddings["ids"]) > 0:
            trace("add_more_embeddings")

        if set(embedding_set["ids"]).intersection(set(self.embeddings["ids"])):
            with pytest.raises(errors.IDAlreadyExistsError):
                self.collection.add(**embedding_set)
            return multiple()
        else:
            self.collection.add(**embedding_set)
            self._add_embeddings(embedding_set)
            return multiple(*embedding_set["ids"])

    @precondition(lambda self: len(self.embeddings["ids"]) > 20)
    @rule(ids=st.lists(consumes(embedding_ids), min_size=1, max_size=20))
    def delete_by_ids(self, ids):
        trace("remove embeddings")

        indices_to_remove = [self.embeddings["ids"].index(id) for id in ids]

        self.collection.delete(ids=ids)
        self._remove_embeddings(set(indices_to_remove))

    # Removing the precondition causes the tests to frequently fail as "unsatisfiable"
    # Using a value < 5 causes retries and lowers the number of valid samples
    @precondition(lambda self: len(self.embeddings["ids"]) >= 5)
    @rule(
        embedding_set=strategies.embedding_set(
            dtype_st=dtype_shared_st,
            dimension_st=dimension_shared_st,
            id_st=embedding_ids,
            count_st=st.integers(min_value=1, max_value=5),
            documents_st_fn=lambda c: st.lists(
                st.text(min_size=1), min_size=c, max_size=c, unique=True
            ),
        )
    )
    def update_embeddings(self, embedding_set):
        trace("update embeddings")
        self.collection.update(**embedding_set)
        self._update_embeddings(embedding_set)

    @invariant()
    def count(self):
        invariants.count(self.api, self.collection.name, len(self.embeddings["ids"]))

    @invariant()
    def no_duplicates(self):
        invariants.no_duplicates(self.collection)

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
            del self.embeddings["embeddings"][i]
            del self.embeddings["metadatas"][i]
            del self.embeddings["documents"][i]

    def _update_embeddings(self, embeddings: strategies.EmbeddingSet):

        for i in range(len(embeddings["ids"])):
            idx = self.embeddings["ids"].index(embeddings["ids"][i])
            if embeddings["embeddings"]:
                self.embeddings["embeddings"][idx] = embeddings["embeddings"][i]
            if embeddings["metadatas"]:
                self.embeddings["metadatas"][idx] = embeddings["metadatas"][i]
            if embeddings["documents"]:
                self.embeddings["documents"][idx] = embeddings["documents"][i]


def test_embeddings_state(caplog, api):
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: EmbeddingStateMachine(api))
    print_traces()


def test_multi_add(api):
    api.reset()
    coll = api.create_collection(name="foo")
    coll.add(ids=["a"], embeddings=[[0.0]])
    assert coll.count() == 1

    with pytest.raises(errors.IDAlreadyExistsError):
        coll.add(ids=["a"], embeddings=[[0.0]])

    assert coll.count() == 1

    results = coll.get()
    assert results["ids"] == ["a"]

    coll.delete(ids=["a"])
    assert coll.count() == 0


def test_dup_add(api):
    api.reset()
    coll = api.create_collection(name="foo")
    with pytest.raises(errors.DuplicateIDError):
        coll.add(ids=["a", "a"], embeddings=[[0.0], [1.1]])


# TODO: Use SQL escaping correctly internally
@pytest.mark.xfail(reason="We don't properly escape SQL internally, causing problems")
def test_escape_chars_in_ids(api):
    api.reset()
    id = "\x1f"
    coll = api.create_collection(name="foo")
    coll.add(ids=[id], embeddings=[[0.0]])
    assert coll.count() == 1
    coll.delete(ids=[id])
    assert coll.count() == 0
