import pytest
import logging
import hypothesis.strategies as st
from typing import Dict, Set, cast, Union, DefaultDict
from dataclasses import dataclass
from chromadb.api.types import ID, Include, IDs
import chromadb.errors as errors
from chromadb.api import API
from chromadb.api.models.Collection import Collection
import chromadb.test.property.strategies as strategies
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    MultipleResults,
    rule,
    initialize,
    precondition,
    consumes,
    run_state_machine_as_test,
    multiple,
    invariant,
)
from collections import defaultdict
import chromadb.test.property.invariants as invariants
import numpy as np


traces: DefaultDict[str, int] = defaultdict(lambda: 0)


def trace(key: str) -> None:
    global traces
    traces[key] += 1


def print_traces() -> None:
    global traces
    for key, value in traces.items():
        print(f"{key}: {value}")


dtype_shared_st: st.SearchStrategy[
    Union[np.float16, np.float32, np.float64]
] = st.shared(st.sampled_from(strategies.float_types), key="dtype")

dimension_shared_st: st.SearchStrategy[int] = st.shared(
    st.integers(min_value=2, max_value=2048), key="dimension"
)


@dataclass
class EmbeddingStateMachineStates:
    initialize = "initialize"
    add_embeddings = "add_embeddings"
    delete_by_ids = "delete_by_ids"
    update_embeddings = "update_embeddings"
    upsert_embeddings = "upsert_embeddings"


collection_st = st.shared(strategies.collections(with_hnsw_params=True), key="coll")


class EmbeddingStateMachine(RuleBasedStateMachine):
    collection: Collection
    embedding_ids: Bundle[ID] = Bundle("embedding_ids")

    def __init__(self, api: API):
        super().__init__()
        self.api = api
        self._rules_strategy = strategies.DeterministicRuleStrategy(self)  # type: ignore

    @initialize(collection=collection_st)  # type: ignore
    def initialize(self, collection: strategies.Collection):
        self.api.reset()
        self.collection = self.api.create_collection(
            name=collection.name,
            metadata=collection.metadata,
            embedding_function=collection.embedding_function,
        )
        self.embedding_function = collection.embedding_function
        trace("init")
        self.on_state_change(EmbeddingStateMachineStates.initialize)

        self.record_set_state = strategies.StateMachineRecordSet(
            ids=[], metadatas=[], documents=[], embeddings=[]
        )

    @rule(target=embedding_ids, record_set=strategies.recordsets(collection_st))
    def add_embeddings(self, record_set: strategies.RecordSet) -> MultipleResults[ID]:
        trace("add_embeddings")
        self.on_state_change(EmbeddingStateMachineStates.add_embeddings)

        normalized_record_set: strategies.NormalizedRecordSet = invariants.wrap_all(
            record_set
        )

        if len(normalized_record_set["ids"]) > 0:
            trace("add_more_embeddings")

        if not invariants.is_metadata_valid(normalized_record_set):
            with pytest.raises(Exception):
                self.collection.add(**normalized_record_set)
            return multiple()

        intersection = set(normalized_record_set["ids"]).intersection(
            self.record_set_state["ids"]
        )
        if len(intersection) > 0:
            # Partially apply the non-duplicative records to the state
            new_ids = list(set(normalized_record_set["ids"]).difference(intersection))
            indices = [normalized_record_set["ids"].index(id) for id in new_ids]
            filtered_record_set: strategies.NormalizedRecordSet = {
                "ids": [normalized_record_set["ids"][i] for i in indices],
                "metadatas": [normalized_record_set["metadatas"][i] for i in indices]
                if normalized_record_set["metadatas"]
                else None,
                "documents": [normalized_record_set["documents"][i] for i in indices]
                if normalized_record_set["documents"]
                else None,
                "embeddings": [normalized_record_set["embeddings"][i] for i in indices]
                if normalized_record_set["embeddings"]
                else None,
            }
            self.collection.add(**normalized_record_set)
            self._upsert_embeddings(cast(strategies.RecordSet, filtered_record_set))
            return multiple(*filtered_record_set["ids"])

        else:
            self.collection.add(**normalized_record_set)
            self._upsert_embeddings(cast(strategies.RecordSet, normalized_record_set))
            return multiple(*normalized_record_set["ids"])

    @precondition(lambda self: len(self.record_set_state["ids"]) > 20)
    @rule(ids=st.lists(consumes(embedding_ids), min_size=1, max_size=20))
    def delete_by_ids(self, ids: IDs) -> None:
        trace("remove embeddings")
        self.on_state_change(EmbeddingStateMachineStates.delete_by_ids)
        indices_to_remove = [self.record_set_state["ids"].index(id) for id in ids]

        self.collection.delete(ids=ids)
        self._remove_embeddings(set(indices_to_remove))

    # Removing the precondition causes the tests to frequently fail as "unsatisfiable"
    # Using a value < 5 causes retries and lowers the number of valid samples
    @precondition(lambda self: len(self.record_set_state["ids"]) >= 5)
    @rule(
        record_set=strategies.recordsets(
            collection_strategy=collection_st,
            id_strategy=embedding_ids,
            min_size=1,
            max_size=5,
        )
    )
    def update_embeddings(self, record_set: strategies.RecordSet) -> None:
        trace("update embeddings")
        self.on_state_change(EmbeddingStateMachineStates.update_embeddings)

        normalized_record_set: strategies.NormalizedRecordSet = invariants.wrap_all(
            record_set
        )
        if not invariants.is_metadata_valid(normalized_record_set):
            with pytest.raises(Exception):
                self.collection.update(**normalized_record_set)
            return

        self.collection.update(**record_set)
        self._upsert_embeddings(record_set)

    # Using a value < 3 causes more retries and lowers the number of valid samples
    @precondition(lambda self: len(self.record_set_state["ids"]) >= 3)
    @rule(
        record_set=strategies.recordsets(
            collection_strategy=collection_st,
            id_strategy=st.one_of(embedding_ids, strategies.safe_text),
            min_size=1,
            max_size=5,
        )
    )
    def upsert_embeddings(self, record_set: strategies.RecordSet) -> None:
        trace("upsert embeddings")
        self.on_state_change(EmbeddingStateMachineStates.upsert_embeddings)

        normalized_record_set: strategies.NormalizedRecordSet = invariants.wrap_all(
            record_set
        )
        if not invariants.is_metadata_valid(normalized_record_set):
            with pytest.raises(Exception):
                self.collection.upsert(**normalized_record_set)
            return

        self.collection.upsert(**record_set)
        self._upsert_embeddings(record_set)

    @invariant()
    def count(self) -> None:
        invariants.count(
            self.collection, cast(strategies.RecordSet, self.record_set_state)
        )

    @invariant()
    def no_duplicates(self) -> None:
        invariants.no_duplicates(self.collection)

    @invariant()
    def ann_accuracy(self) -> None:
        invariants.ann_accuracy(
            collection=self.collection,
            record_set=cast(strategies.RecordSet, self.record_set_state),
            min_recall=0.95,
            embedding_function=self.embedding_function,
        )

    @invariant()
    def fields_match(self) -> None:
        self.record_set_state = cast(strategies.RecordSet, self.record_set_state)
        invariants.embeddings_match(self.collection, self.record_set_state)
        invariants.metadatas_match(self.collection, self.record_set_state)
        invariants.documents_match(self.collection, self.record_set_state)

    def _upsert_embeddings(self, record_set: strategies.RecordSet) -> None:
        normalized_record_set: strategies.NormalizedRecordSet = invariants.wrap_all(
            record_set
        )
        for idx, id in enumerate(normalized_record_set["ids"]):
            # Update path
            if id in self.record_set_state["ids"]:
                target_idx = self.record_set_state["ids"].index(id)
                if normalized_record_set["embeddings"] is not None:
                    self.record_set_state["embeddings"][
                        target_idx
                    ] = normalized_record_set["embeddings"][idx]
                else:
                    assert normalized_record_set["documents"] is not None
                    assert self.embedding_function is not None
                    self.record_set_state["embeddings"][
                        target_idx
                    ] = self.embedding_function(
                        [normalized_record_set["documents"][idx]]
                    )[
                        0
                    ]
                if normalized_record_set["metadatas"] is not None:
                    # Sqlite merges the metadata, as opposed to old
                    # implementations which overwrites it
                    record_set_state = self.record_set_state["metadatas"][target_idx]
                    if record_set_state is not None:
                        record_set_state = cast(
                            Dict[str, Union[str, int, float]], record_set_state
                        )
                        record_set_state.update(normalized_record_set["metadatas"][idx])
                if normalized_record_set["documents"] is not None:
                    self.record_set_state["documents"][
                        target_idx
                    ] = normalized_record_set["documents"][idx]
            else:
                # Add path
                self.record_set_state["ids"].append(id)
                if normalized_record_set["embeddings"] is not None:
                    self.record_set_state["embeddings"].append(
                        normalized_record_set["embeddings"][idx]
                    )
                else:
                    assert self.embedding_function is not None
                    assert normalized_record_set["documents"] is not None
                    self.record_set_state["embeddings"].append(
                        self.embedding_function(
                            [normalized_record_set["documents"][idx]]
                        )[0]
                    )
                if normalized_record_set["metadatas"] is not None:
                    self.record_set_state["metadatas"].append(
                        normalized_record_set["metadatas"][idx]
                    )
                else:
                    self.record_set_state["metadatas"].append(None)
                if normalized_record_set["documents"] is not None:
                    self.record_set_state["documents"].append(
                        normalized_record_set["documents"][idx]
                    )
                else:
                    self.record_set_state["documents"].append(None)

    def _remove_embeddings(self, indices_to_remove: Set[int]) -> None:
        indices_list = list(indices_to_remove)
        indices_list.sort(reverse=True)

        for i in indices_list:
            del self.record_set_state["ids"][i]
            del self.record_set_state["embeddings"][i]
            del self.record_set_state["metadatas"][i]
            del self.record_set_state["documents"][i]

    def on_state_change(self, new_state: str) -> None:
        pass


def test_embeddings_state(caplog: pytest.LogCaptureFixture, api: API) -> None:
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: EmbeddingStateMachine(api))  # type: ignore
    print_traces()


def test_multi_add(api: API) -> None:
    api.reset()
    coll = api.create_collection(name="foo")
    coll.add(ids=["a"], embeddings=[[0.0]])
    assert coll.count() == 1

    # after the sqlite refactor - add silently ignores duplicates, no exception is raised
    # partial adds are supported - i.e we will add whatever we can in the request
    coll.add(ids=["a"], embeddings=[[0.0]])

    assert coll.count() == 1

    results = coll.get()
    assert results["ids"] == ["a"]

    coll.delete(ids=["a"])
    assert coll.count() == 0


def test_dup_add(api: API) -> None:
    api.reset()
    coll = api.create_collection(name="foo")
    with pytest.raises(errors.DuplicateIDError):
        coll.add(ids=["a", "a"], embeddings=[[0.0], [1.1]])
    with pytest.raises(errors.DuplicateIDError):
        coll.upsert(ids=["a", "a"], embeddings=[[0.0], [1.1]])


def test_query_without_add(api: API) -> None:
    api.reset()
    coll = api.create_collection(name="foo")
    fields: Include = ["documents", "metadatas", "embeddings", "distances"]
    N = np.random.randint(1, 2000)
    K = np.random.randint(1, 100)
    results = coll.query(
        query_embeddings=np.random.random((N, K)).tolist(), include=fields
    )
    for field in fields:
        field_results = results[field]
        assert field_results is not None
        assert all([len(result) == 0 for result in field_results])


def test_get_non_existent(api: API) -> None:
    api.reset()
    coll = api.create_collection(name="foo")
    result = coll.get(ids=["a"], include=["documents", "metadatas", "embeddings"])
    assert len(result["ids"]) == 0
    assert len(result["metadatas"]) == 0
    assert len(result["documents"]) == 0
    assert len(result["embeddings"]) == 0


# TODO: Use SQL escaping correctly internally
@pytest.mark.xfail(reason="We don't properly escape SQL internally, causing problems")
def test_escape_chars_in_ids(api: API) -> None:
    api.reset()
    id = "\x1f"
    coll = api.create_collection(name="foo")
    coll.add(ids=[id], embeddings=[[0.0]])
    assert coll.count() == 1
    coll.delete(ids=[id])
    assert coll.count() == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"ids": []},
        {"where": {}},
        {"where_document": {}},
        {"where_document": {}, "where": {}},
    ],
)
def test_delete_empty_fails(api: API, kwargs: dict):
    api.reset()
    coll = api.create_collection(name="foo")
    with pytest.raises(Exception) as e:
        coll.delete(**kwargs)
    assert "You must provide either ids, where, or where_document to delete." in str(e)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"ids": ["foo"]},
        {"where": {"foo": "bar"}},
        {"where_document": {"$contains": "bar"}},
        {"ids": ["foo"], "where": {"foo": "bar"}},
        {"ids": ["foo"], "where_document": {"$contains": "bar"}},
        {
            "ids": ["foo"],
            "where": {"foo": "bar"},
            "where_document": {"$contains": "bar"},
        },
    ],
)
def test_delete_success(api: API, kwargs: dict):
    api.reset()
    coll = api.create_collection(name="foo")
    # Should not raise
    coll.delete(**kwargs)
