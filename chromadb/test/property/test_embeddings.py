import hypothesis.stateful
import hypothesis.strategies
from overrides import overrides
import pytest
import logging
import hypothesis
import hypothesis.strategies as st
from hypothesis import given, settings, HealthCheck
from typing import Dict, Set, cast, Union, DefaultDict, Any, List
from dataclasses import dataclass
from chromadb.api.types import (
    ID,
    Include,
    IDs,
    Embeddings,
    validate_embeddings,
    maybe_cast_one_to_many_embedding,
)
from chromadb.config import System
import chromadb.errors as errors
from chromadb.api import ClientAPI
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
from chromadb.test.conftest import is_client_in_process, reset, NOT_CLUSTER_ONLY
import numpy as np
import uuid
from chromadb.test.utils.wait_for_version_increase import (
    wait_for_version_increase,
    get_collection_version,
)


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


class EmbeddingStateMachineBase(RuleBasedStateMachine):
    collection: Collection
    embedding_ids: Bundle[ID] = Bundle("embedding_ids")
    has_collection_mutated = False

    def __init__(self, client: ClientAPI):
        super().__init__()
        self.client = client
        self._rules_strategy = hypothesis.stateful.RuleStrategy(self)  # type: ignore

    @initialize(collection=collection_st)  # type: ignore
    def initialize(self, collection: strategies.Collection):
        reset(self.client)
        self.collection = self.client.create_collection(
            name=collection.name,
            metadata=collection.metadata,  # type: ignore[arg-type]
            embedding_function=collection.embedding_function,
        )
        self.embedding_function = collection.embedding_function
        trace("init")
        self.on_state_change(EmbeddingStateMachineStates.initialize)

        self.record_set_state = strategies.StateMachineRecordSet(
            ids=[], metadatas=[], documents=[], embeddings=[]
        )

    @overrides
    def teardown(self) -> None:
        self.client.delete_collection(self.collection.name)

    @rule(
        target=embedding_ids,
        record_set=strategies.recordsets(collection_st, can_ids_be_empty=True),
    )
    def add_embeddings(self, record_set: strategies.RecordSet) -> MultipleResults[ID]:
        trace("add_embeddings")
        self.on_state_change(EmbeddingStateMachineStates.add_embeddings)

        normalized_record_set: strategies.NormalizedRecordSet = invariants.wrap_all(
            record_set
        )

        ids = normalized_record_set["ids"]
        s_ids = self.record_set_state["ids"]

        intersection = set()
        # only find possible intersection when
        # record_set ids is not None and record_set_state ids is not None
        if ids is not None and s_ids is not None:
            intersection = set(ids).intersection(s_ids)

        # if there is an intersection, we need to apply the non-duplicative records to the state
        if ids is not None and s_ids is not None and len(intersection) > 0:
            # Partially apply the non-duplicative records to the state
            new_ids = list(set(ids).difference(intersection))
            indices = [ids.index(id) for id in new_ids]
            filtered_record_set: strategies.NormalizedRecordSet = {
                "ids": [ids[i] for i in indices],
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
            result = self.collection.add(**normalized_record_set)  # type: ignore[arg-type]

            self._upsert_embeddings(cast(strategies.RecordSet, filtered_record_set))
            return multiple(*filtered_record_set["ids"])  # type: ignore

        # if there is no intersection, we can apply the entire record set to the state
        else:
            result = self.collection.add(**normalized_record_set)  # type: ignore[arg-type]

            if normalized_record_set["ids"] is None:
                normalized_record_set["ids"] = result["ids"]

            self._upsert_embeddings(cast(strategies.RecordSet, normalized_record_set))
            return multiple(*normalized_record_set["ids"])  # type: ignore

    @rule(ids=st.lists(consumes(embedding_ids), min_size=1))
    def delete_by_ids(self, ids: IDs) -> None:
        trace("remove embeddings")
        self.on_state_change(EmbeddingStateMachineStates.delete_by_ids)

        normalized_ids = self.record_set_state["ids"]
        if normalized_ids is None:
            return

        indices_to_remove = [normalized_ids.index(id) for id in ids]

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
        ),
    )
    def update_embeddings(self, record_set: strategies.RecordSet) -> None:
        trace("update embeddings")
        self.on_state_change(EmbeddingStateMachineStates.update_embeddings)

        self.collection.update(**record_set)  # type: ignore[arg-type]
        self._upsert_embeddings(record_set)

    # Using a value < 3 causes more retries and lowers the number of valid samples
    @precondition(
        lambda self: len(self.record_set_state["ids"]) >= 3
        if self.record_set_state["ids"] is not None
        else False
    )
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

        self.collection.upsert(**record_set)  # type: ignore[arg-type]
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
        self.record_set_state = cast(strategies.RecordSet, self.record_set_state)  # type: ignore[assignment]
        invariants.embeddings_match(self.collection, self.record_set_state)  # type: ignore[arg-type]
        invariants.metadatas_match(self.collection, self.record_set_state)  # type: ignore[arg-type]
        invariants.documents_match(self.collection, self.record_set_state)  # type: ignore[arg-type]

    @precondition(
        lambda self: is_client_in_process(self.client)
    )  # (Can't check the log size on HTTP clients)
    @invariant()
    def log_size_below_max(self) -> None:
        system: System = self.client._system  # type: ignore
        invariants.log_size_below_max(
            system, [self.collection], self.has_collection_mutated
        )

    def _upsert_embeddings(self, record_set: strategies.RecordSet) -> None:
        normalized_record_set: strategies.NormalizedRecordSet = invariants.wrap_all(
            record_set
        )

        if normalized_record_set["ids"] is None:
            return

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
                        if normalized_record_set["metadatas"][idx] is not None:
                            record_set_state.update(
                                normalized_record_set["metadatas"][idx]  # type: ignore[arg-type]
                            )
                        else:
                            # None in the update metadata is a no-op
                            pass
                    else:
                        self.record_set_state["metadatas"][
                            target_idx
                        ] = normalized_record_set["metadatas"][idx]
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
        if new_state != EmbeddingStateMachineStates.initialize:
            self.has_collection_mutated = True


class EmbeddingStateMachine(EmbeddingStateMachineBase):
    embedding_ids: Bundle[ID] = Bundle("embedding_ids")

    def __init__(self, client: ClientAPI):
        super().__init__(client)

    @initialize(collection=collection_st)  # type: ignore
    def initialize(self, collection: strategies.Collection):
        super().initialize(collection)
        print(
            "[test_embeddings][initialize] Initialize collection id ",
            self.collection._model["id"],
            " hypothesis generated collection id ",
            collection.id,
        )
        self.log_operation_count = 0
        self.unique_ids_in_log: Set[ID] = set()
        self.collection_version = self.collection.get_model()["version"]

    @precondition(
        lambda self: not NOT_CLUSTER_ONLY
        and self.log_operation_count > 10
        and len(self.unique_ids_in_log) > 3
    )
    @rule()
    def wait_for_compaction(self) -> None:
        current_version = get_collection_version(self.client, self.collection.name)
        assert current_version >= self.collection_version  # type: ignore[operator]
        # This means that there was a compaction from the last time this was
        # invoked. Ok to start all over again.
        if current_version > self.collection_version:  # type: ignore[operator]
            print(
                "[test_embeddings][wait_for_compaction] collection version has changed, so reset to 0"
            )
            self.collection_version = current_version
            # This is fine even if the log has some records right now
            self.log_operation_count = 0
            self.unique_ids_in_log = set()
        else:
            print(
                "[test_embeddings][wait_for_compaction] wait for version to increase from current version ",
                current_version,
            )
            new_version = wait_for_version_increase(
                self.client, self.collection.name, current_version, additional_time=240
            )
            # Everything got compacted.
            self.log_operation_count = 0
            self.unique_ids_in_log = set()
            self.collection_version = new_version

    @rule(
        target=embedding_ids,
        record_set=strategies.recordsets(collection_st, can_ids_be_empty=True),
    )
    def add_embeddings(self, record_set: strategies.RecordSet) -> MultipleResults[ID]:
        res = super().add_embeddings(record_set)
        normalized_record_set: strategies.NormalizedRecordSet = invariants.wrap_all(
            record_set
        )

        ids = normalized_record_set["ids"]
        n_ids = len(invariants.wrap(res))

        print(
            "[test_embeddings][add] Non Intersection ids ",
            ids,
            " len ",
            n_ids,
        )
        self.log_operation_count += n_ids

        if ids is None:
            return res  # type: ignore[return-value]

        for id in ids:
            if id not in self.unique_ids_in_log:
                self.unique_ids_in_log.add(id)
        return res  # type: ignore[return-value]

    @rule(ids=st.lists(consumes(embedding_ids), min_size=1))
    def delete_by_ids(self, ids: IDs) -> None:
        super().delete_by_ids(ids)
        print("[test_embeddings][delete] ids ", ids, " len ", len(ids))
        self.log_operation_count += len(ids)
        for id in ids:
            if id in self.unique_ids_in_log:
                self.unique_ids_in_log.remove(id)

    # Removing the precondition causes the tests to frequently fail as "unsatisfiable"
    # Using a value < 5 causes retries and lowers the number of valid samples
    @precondition(lambda self: len(self.record_set_state["ids"]) >= 5)
    @rule(
        record_set=strategies.recordsets(
            collection_strategy=collection_st,
            id_strategy=embedding_ids,
            min_size=1,
            max_size=5,
        ),
    )
    def update_embeddings(self, record_set: strategies.RecordSet) -> None:
        super().update_embeddings(record_set)

        n = (
            len(invariants.wrap(record_set["ids"]))
            if record_set["ids"] is not None
            else 0
        )
        print(
            "[test_embeddings][update] ids ",
            record_set["ids"],
            " len ",
            n,
        )
        self.log_operation_count += n

    # Using a value < 3 causes more retries and lowers the number of valid samples
    @precondition(
        lambda self: len(
            self.record_set_state["ids"]
            if self.record_set_state["ids"] is not None
            else []
        )
        >= 3
    )
    @rule(
        record_set=strategies.recordsets(
            collection_strategy=collection_st,
            id_strategy=st.one_of(embedding_ids, strategies.safe_text),
            min_size=1,
            max_size=5,
        )
    )
    def upsert_embeddings(self, record_set: strategies.RecordSet) -> None:
        super().upsert_embeddings(record_set)

        normalized_ids = (
            invariants.wrap(record_set["ids"]) if record_set["ids"] is not None else []
        )
        n_ids = len(normalized_ids)

        print(
            "[test_embeddings][upsert] ids ",
            record_set["ids"],
            " len ",
            n_ids,
        )
        self.log_operation_count += n_ids
        for id in normalized_ids:
            if id not in self.unique_ids_in_log:
                self.unique_ids_in_log.add(id)


def test_embeddings_state(caplog: pytest.LogCaptureFixture, client: ClientAPI) -> None:
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(
        lambda: EmbeddingStateMachine(client),
        settings=settings(
            deadline=90000, suppress_health_check=[HealthCheck.filter_too_much]
        ),
    )  # type: ignore
    print_traces()


def test_add_then_delete_n_minus_1(client: ClientAPI) -> None:
    state = EmbeddingStateMachine(client)
    state.initialize(
        collection=strategies.Collection(
            name="A00",
            metadata={
                "hnsw:construction_ef": 128,
                "hnsw:search_ef": 128,
                "hnsw:M": 128,
            },
            embedding_function=None,
            id=uuid.uuid4(),
            dimension=2,
            dtype=np.float16,
            known_metadata_keys={},
            known_document_keywords=[],
            has_documents=False,
            has_embeddings=True,
        )
    )
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    v1, v2, v3, v4, v5, v6 = state.add_embeddings(  # type: ignore[misc]
        record_set={
            "ids": ["0", "1", "2", "3", "4", "5"],
            "embeddings": [
                [0.09765625, 0.430419921875],
                [0.20556640625, 0.08978271484375],
                [-0.1527099609375, 0.291748046875],
                [-0.12481689453125, 0.78369140625],
                [0.92724609375, -0.233154296875],
                [0.92724609375, -0.233154296875],
            ],
            "metadatas": [None, None, None, None, None, None],
            "documents": None,
        }
    )
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    state.delete_by_ids(ids=[v1, v2, v3, v4, v5])
    if not NOT_CLUSTER_ONLY:
        state.wait_for_compaction()
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    state.teardown()


def test_update_none(caplog: pytest.LogCaptureFixture, client: ClientAPI) -> None:
    state = EmbeddingStateMachine(client)
    state.initialize(
        collection=strategies.Collection(
            name="A00",
            metadata={
                "hnsw:construction_ef": 128,
                "hnsw:search_ef": 128,
                "hnsw:M": 128,
            },
            embedding_function=None,
            id=uuid.UUID("2fb0c945-b877-42ab-9417-bfe0f6b172af"),
            dimension=2,
            dtype=np.float16,
            known_metadata_keys={},
            known_document_keywords=[],
            has_documents=False,
            has_embeddings=True,
        )
    )
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    v1, v2, v3, v4, v5 = state.add_embeddings(  # type: ignore[misc]
        record_set={
            "ids": ["0", "1", "2", "3", "4"],
            "embeddings": [
                [0.09765625, 0.430419921875],
                [0.20556640625, 0.08978271484375],
                [-0.1527099609375, 0.291748046875],
                [-0.12481689453125, 0.78369140625],
                [0.92724609375, -0.233154296875],
            ],
            "metadatas": [None, None, None, None, None],
            "documents": None,
        }
    )
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    state.update_embeddings(
        record_set={
            "ids": [v5],
            "embeddings": [[0.58349609375, 0.05780029296875]],
            "metadatas": [{v1: v1}],
            "documents": None,
        }
    )
    state.ann_accuracy()
    state.teardown()


def test_add_delete_add(client: ClientAPI) -> None:
    state = EmbeddingStateMachine(client)
    state.initialize(
        collection=strategies.Collection(
            name="KR3cf",
            metadata={
                "Ufmxsi3": 999999.0,
                "bMMvvrqM4MKmp5CJB8A": 62921,
                "-": True,
                "37PNi": "Vkn",
                "5KZfkpod3ND5soL_": True,
                "KA4zcZL9lRN9": 142,
                "Oc8G7ysXmE8lp4Hos_": "POQe8Unz1uJ",
                "BI930U": 31,
                "te": False,
                "tyM": -0.5,
                "R0ZiZ": True,
                "m": True,
                "IOw": -25725,
                "hnsw:construction_ef": 128,
                "hnsw:search_ef": 128,
                "hnsw:M": 128,
            },
            embedding_function=None,
            id=uuid.UUID("284b6e99-b19e-49b2-96a4-a2a93a95447d"),
            dimension=3,
            dtype=np.float32,
            known_metadata_keys={},
            known_document_keywords=[],
            has_documents=False,
            has_embeddings=True,
        )
    )
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    embeddings = state.add_embeddings(
        record_set={
            "ids": ["255", "l", "3-", "i", "Nk", "9yPvT"],
            "embeddings": [
                [1.2, 2.3, 1.5],
                [4.5, 6.0, 2],
                [1, 2, 3],
                [4, 5, 6],
                [8.9, 9.0, 7],
                [4.5, 6.0, 5.6],
            ],
            "metadatas": None,
            "documents": None,
        }
    )
    i = 0
    emb_list = {}
    for embedding in embeddings:
        emb_list[i] = embedding
        i += 1
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    state.upsert_embeddings(
        record_set={
            "ids": [
                emb_list[0],
                emb_list[4],
                "KWcDaHUVD6MxEiJ",
                emb_list[5],
                "PdlP1d6w",
            ],
            "embeddings": [[1, 23, 4], [3, 5, 9], [9, 3, 5], [3, 9, 8], [1, 5, 4]],
            "documents": None,
            "metadatas": None,
        }
    )
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    if not NOT_CLUSTER_ONLY:
        state.wait_for_compaction()
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    state.upsert_embeddings(
        record_set={
            "ids": ["TpjiboLSuYWBJDbRW1zeNmC", emb_list[0], emb_list[4]],
            "embeddings": [[4, 6, 7], [7, 9, 3], [1, 3, 6]],
            "metadatas": None,
            "documents": None,
        }
    )
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    state.delete_by_ids(
        ids=[emb_list[2], emb_list[1], emb_list[5], emb_list[4], emb_list[3]]
    )
    state.ann_accuracy()
    state.count()
    state.fields_match()
    state.no_duplicates()
    embeddings = state.add_embeddings(
        record_set={
            "ids": ["o", "D3V84", "Rt", "TDwlc9C8_evn", emb_list[1]],
            "embeddings": [
                [9, 5.4, 3.22],
                [1.33, 3.44, 5.66],
                [9.90, 9.8, 1.3],
                [9.7, 5.6, 4.5],
                [3.4, 5.6, 9.65],
            ],
            "documents": None,
            "metadatas": None,
        }
    )
    i = 6
    for embedding in embeddings:
        emb_list[i] = embedding
        i += 1
    state.ann_accuracy()
    state.count()
    state.fields_match()
    if not NOT_CLUSTER_ONLY:
        state.wait_for_compaction()


def test_multi_add(client: ClientAPI) -> None:
    reset(client)
    coll = client.create_collection(name="foo")
    coll.add(ids=["a"], embeddings=[[0.0]])  # type: ignore[arg-type]
    assert coll.count() == 1

    # after the sqlite refactor - add silently ignores duplicates, no exception is raised
    # partial adds are supported - i.e we will add whatever we can in the request
    coll.add(ids=["a"], embeddings=[[0.0]])  # type: ignore[arg-type]

    assert coll.count() == 1

    results = coll.get()
    assert results["ids"] == ["a"]

    coll.delete(ids=["a"])
    assert coll.count() == 0


def test_dup_add(client: ClientAPI) -> None:
    reset(client)
    coll = client.create_collection(name="foo")
    with pytest.raises(errors.DuplicateIDError):
        coll.add(ids=["a", "a"], embeddings=[[0.0], [1.1]])  # type: ignore[arg-type]
    with pytest.raises(errors.DuplicateIDError):
        coll.upsert(ids=["a", "a"], embeddings=[[0.0], [1.1]])  # type: ignore[arg-type]


def test_query_without_add(client: ClientAPI) -> None:
    reset(client)
    coll = client.create_collection(name="foo")
    fields: Include = ["documents", "metadatas", "embeddings", "distances"]  # type: ignore[list-item]
    N = np.random.randint(1, 2000)
    K = np.random.randint(1, 100)
    results = coll.query(
        query_embeddings=np.random.random((N, K)).tolist(), include=fields
    )
    for field in fields:
        field_results = results[field]  # type: ignore[literal-required]
        assert field_results is not None
        assert all([len(result) == 0 for result in field_results])


def test_get_non_existent(client: ClientAPI) -> None:
    reset(client)
    coll = client.create_collection(name="foo")
    result = coll.get(ids=["a"], include=["documents", "metadatas", "embeddings"])  # type: ignore[list-item]
    assert len(result["ids"]) == 0
    assert len(result["metadatas"]) == 0  # type: ignore[arg-type]
    assert len(result["documents"]) == 0  # type: ignore[arg-type]
    assert len(result["embeddings"]) == 0  # type: ignore[arg-type]


# TODO: Use SQL escaping correctly internally
@pytest.mark.xfail(reason="We don't properly escape SQL internally, causing problems")
def test_escape_chars_in_ids(client: ClientAPI) -> None:
    reset(client)
    id = "\x1f"
    coll = client.create_collection(name="foo")
    coll.add(ids=[id], embeddings=[[0.0]])  # type: ignore[arg-type]
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
def test_delete_empty_fails(client: ClientAPI, kwargs: Any) -> None:
    reset(client)
    coll = client.create_collection(name="foo")
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
def test_delete_success(client: ClientAPI, kwargs: Any) -> None:
    reset(client)
    coll = client.create_collection(name="foo")
    # Should not raise
    coll.delete(**kwargs)


@given(supported_types=st.sampled_from([np.float32, np.int32, np.int64, int, float]))
def test_autocasting_validate_embeddings_for_compatible_types(
    supported_types: List[Any],
) -> None:
    embds = strategies.create_embeddings(10, 10, supported_types)
    validated_embeddings = validate_embeddings(maybe_cast_one_to_many_embedding(embds))  # type: ignore[arg-type]
    assert all(
        [
            isinstance(value, np.ndarray)
            and (
                value.dtype == np.float32
                or value.dtype == np.float64
                or value.dtype == np.int32
                or value.dtype == np.int64
            )
            for value in validated_embeddings
        ]
    )


@given(supported_types=st.sampled_from([np.float32, np.int32, np.int64, int, float]))
def test_autocasting_validate_embeddings_with_ndarray(
    supported_types: List[Any],
) -> None:
    embds = strategies.create_embeddings_ndarray(10, 10, supported_types)
    validated_embeddings = validate_embeddings(maybe_cast_one_to_many_embedding(embds))  # type: ignore[arg-type]
    assert all(
        [
            isinstance(value, np.ndarray)
            and (
                value.dtype == np.float32
                or value.dtype == np.float64
                or value.dtype == np.int32
                or value.dtype == np.int64
            )
            for value in validated_embeddings
        ]
    )


@given(unsupported_types=st.sampled_from([str, bool]))
def test_autocasting_validate_embeddings_incompatible_types(
    unsupported_types: List[Any],
) -> None:
    embds = strategies.create_embeddings(10, 10, unsupported_types)
    with pytest.raises(ValueError) as e:
        validate_embeddings(maybe_cast_one_to_many_embedding(embds))  # type: ignore[arg-type]

    assert "Expected each value in the embedding to be a int or float" in str(e)


def test_0dim_embedding_validation() -> None:
    embds: Embeddings = [np.array([])]
    with pytest.raises(ValueError) as e:
        validate_embeddings(embds)
    assert (
        "Expected each embedding in the embeddings to be a 1-dimensional numpy array with at least 1 int/float value. Got a 1-dimensional numpy array with no values at pos"
        in str(e)
    )
