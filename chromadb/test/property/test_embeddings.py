import pytest
import logging
from hypothesis import given
import hypothesis.strategies as st
from typing import Set, Optional
import chromadb
import chromadb.errors as errors
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from chromadb.test.configurations import configurations
import chromadb.test.property.strategies as strategies
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
import chromadb.test.property.invariants as invariants
import hypothesis


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
dimension_shared_st = st.shared(
    st.integers(min_value=2, max_value=2048), key="dimension"
)


collection_st = st.shared(strategies.collections(), key="coll")

class EmbeddingStateMachine(RuleBasedStateMachine):
    collection: Collection
    embedding_ids: Bundle = Bundle("embedding_ids")

    def __init__(self, api = None):
        super().__init__()
        if not api:
            api = chromadb.Client(configurations()[0])
        self.api = api
        self._rules_strategy = MyRuleStrategy(self)

    @initialize(collection=collection_st)
    def initialize(self, collection: strategies.Collection):
        self.api.reset()
        self.collection = self.api.create_collection(name=collection.name,
                                                     metadata=collection.metadata,
                                                     embedding_function=collection.ef)
        trace("init")
        self.embeddings = {
            "ids": [],
            "embeddings": [],
            "metadatas": [],
            "documents": [],
        }

    @rule(target=embedding_ids,
          embedding_set=strategies.recordsets(collection_st))
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
            self._upsert_embeddings(embedding_set)
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
    @rule(embedding_set=strategies.recordsets(collection_strategy=collection_st,
                                              id_strategy=embedding_ids,
                                              min_size=1,
                                              max_size=5))
    def update_embeddings(self, embedding_set):
        trace("update embeddings")
        self.collection.update(**embedding_set)
        self._upsert_embeddings(embedding_set)

    # Using a value < 3 causes more retries and lowers the number of valid samples
    @precondition(lambda self: len(self.embeddings["ids"]) >= 3)
    @rule(embedding_set=strategies.recordsets(
              collection_strategy=collection_st,
              id_strategy=st.one_of(embedding_ids, strategies.safe_text),
              min_size=1, max_size=5))
    def upsert_embeddings(self, embedding_set):
        trace("upsert embeddings")
        self.collection.upsert(**embedding_set)
        self._upsert_embeddings(embedding_set)

    @invariant()
    def count(self):
        invariants.count(self.api, self.collection.name, len(self.embeddings["ids"]))

    @invariant()
    def no_duplicates(self):
        invariants.no_duplicates(self.collection)

    @invariant()
    def ann_accuracy(self):
        invariants.ann_accuracy(
            collection=self.collection, embeddings=self.embeddings, min_recall=0.95  #type: ignore
        )

    def _upsert_embeddings(self, embeddings: strategies.RecordSet):
        for idx, id in enumerate(embeddings["ids"]):
            if id in self.embeddings["ids"]:
                target_idx = self.embeddings["ids"].index(id)
                if "embeddings" in embeddings and embeddings["embeddings"] is not None:
                    self.embeddings["embeddings"][target_idx] = embeddings["embeddings"][idx]
                if "metadatas" in embeddings and embeddings["metadatas"] is not None:
                    self.embeddings["metadatas"][target_idx] = embeddings["metadatas"][idx]
                if "documents" in embeddings and embeddings["documents"] is not None:
                    self.embeddings["documents"][target_idx] = embeddings["documents"][idx]
            else:
                self.embeddings["ids"].append(id)
                if "embeddings" in embeddings and embeddings["embeddings"] is not None:
                    self.embeddings["embeddings"].append(embeddings["embeddings"][idx])
                else:
                    self.embeddings["embeddings"].append(None)
                if "metadatas" in embeddings and embeddings["metadatas"] is not None:
                    self.embeddings["metadatas"].append(embeddings["metadatas"][idx])
                else:
                    self.embeddings["metadatas"].append(None)
                if "documents" in embeddings and embeddings["documents"] is not None:
                    self.embeddings["documents"].append(embeddings["documents"][idx])
                else:
                    self.embeddings["documents"].append(None)

    def _remove_embeddings(self, indices_to_remove: Set[int]):
        indices_list = list(indices_to_remove)
        indices_list.sort(reverse=True)

        for i in indices_list:
            del self.embeddings["ids"][i]
            del self.embeddings["embeddings"][i]
            del self.embeddings["metadatas"][i]
            del self.embeddings["documents"][i]

def test_embeddings_state(caplog, api):
    caplog.set_level(logging.ERROR)
    run_state_machine_as_test(lambda: EmbeddingStateMachine(api))
    print_traces()

TestEmbeddingsState = EmbeddingStateMachine.TestCase

def test_multi_add(api: API):
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


def test_dup_add(api: API):
    api.reset()
    coll = api.create_collection(name="foo")
    with pytest.raises(errors.DuplicateIDError):
        coll.add(ids=["a", "a"], embeddings=[[0.0], [1.1]])
    with pytest.raises(errors.DuplicateIDError):
        coll.upsert(ids=["a", "a"], embeddings=[[0.0], [1.1]])


# TODO: Use SQL escaping correctly internally
@pytest.mark.xfail(reason="We don't properly escape SQL internally, causing problems")
def test_escape_chars_in_ids(api: API):
    api.reset()
    id = "\x1f"
    coll = api.create_collection(name="foo")
    coll.add(ids=[id], embeddings=[[0.0]])
    assert coll.count() == 1
    coll.delete(ids=[id])
    assert coll.count() == 0


# ==========================================================

@st.composite
def often_invalid(draw):
    n = draw(st.lists(st.text(), min_size=5, unique=True))
    return n

@given(es=strategies.recordsets())
def test_generate_embeddings(es):
    assert es is not None


@given(data=often_invalid())
def test_hypothesis(data):
    assert data is not None



class MyStateMachine(RuleBasedStateMachine):

    def __init__(self) -> None:
        super().__init__()
        self._rules_strategy = MyRuleStrategy(self)

    #@rule(my_data=strategies.dictionaries(strategies.safe_text, st.one_of(*strategies.safe_values)))
    #@rule(my_data=st.lists(st.text(), min_size=1, unique=False))
    @rule(my_data=st.text())
    def rule1(self, my_data):
       #print("IN RULE 1:", my_data)
       assert my_data is not None


TestMyStateMachine = MyStateMachine.TestCase



from hypothesis.strategies._internal.strategies import SearchStrategy
from hypothesis.strategies._internal.featureflags import FeatureStrategy
from hypothesis.errors import InvalidArgument, InvalidDefinition

class MyRuleStrategy(SearchStrategy):
    def __init__(self, machine):
        super().__init__()
        self.machine = machine
        self.rules = list(machine.rules())

        # The order is a bit arbitrary. Primarily we're trying to group rules
        # that write to the same location together, and to put rules with no
        # target first as they have less effect on the structure. We order from
        # fewer to more arguments on grounds that it will plausibly need less
        # data. This probably won't work especially well and we could be
        # smarter about it, but it's better than just doing it in definition
        # order.
        self.rules.sort(
            key=lambda rule: (
                sorted(rule.targets),
                len(rule.arguments),
                rule.function.__name__,
            )
        )

    def __repr__(self):
        return "{}(machine={}({{...}}))".format(
            self.__class__.__name__,
            self.machine.__class__.__name__,
        )

    def do_draw(self, data):
        if not any(self.is_valid(rule) for rule in self.rules):
            msg = f"No progress can be made from state {self.machine!r}"
            raise InvalidDefinition(msg) from None

        # Note: The order of the filters here is actually quite important,
        # because checking is_enabled makes choices, so increases the size of
        # the choice sequence. This means that if we are in a case where many
        # rules are invalid we will make a lot more choices if we ask if they
        # are enabled before we ask if they are valid, so our test cases will
        # be artificially large.

        rule = data.draw(
            st.sampled_from([r for r in self.rules if self.is_valid(r)])
        )

        #print("pre-arg-draw")
        argdata = data.draw(rule.arguments_strategy)
        #print("post-arg-draw: ", argdata)

        return (rule, argdata)

    def is_valid(self, rule):
        if not all(precond(self.machine) for precond in rule.preconditions):
            return False

        for b in rule.bundles:
            bundle = self.machine.bundle(b.name)
            if not bundle:
                return False
        return True