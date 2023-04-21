import hypothesis
import hypothesis.strategies as st
from typing import Optional, TypedDict, Callable, List, Dict, Union, cast, TypeVar
import hypothesis.extra.numpy as npst
import numpy as np
import chromadb.api.types as types
import re
from hypothesis.strategies._internal.strategies import SearchStrategy
from hypothesis.strategies._internal.featureflags import FeatureStrategy
from hypothesis.errors import InvalidArgument, InvalidDefinition

from dataclasses import dataclass

# Set the random seed for reproducibility
np.random.seed(0) # unnecessary, hypothesis does this for us

# See Hypothesis documentation for creating strategies at
# https://hypothesis.readthedocs.io/en/latest/data.html

# NOTE: Because these strategies are used in state machines, we need to
# work around an issue with state machines, in which strategies that frequently
# are marked as invalid (i.e. through the use of `assume` or `.filter`) can cause the
# state machine tests to fail with an hypothesis.errors.Unsatisfiable.

# Ultimately this is because the entire state machine is run as a single Hypothesis
# example, which ends up drawing from the same strategies an enormous number of times.
# Whenever a strategy marks itself as invalid, Hypothesis tries to start the entire
# state machine run over. See https://github.com/HypothesisWorks/hypothesis/issues/3618

# Because strategy generation is all interrelated, seemingly small changes (especially
# ones called early in a test) can have an outside effect. Generating lists with
# unique=True, or dictionaries with a min size seems especially bad.

# Please make changes to these strategies incrementally, testing to make sure they don't
# start generating unsatisfiable examples.


class RecordSet(TypedDict):
    """
    A generated set of embeddings, ids, metadatas, and documents that
    represent what a user would pass to the API.
    """
    ids: types.IDs
    embeddings: Optional[types.Embeddings]
    metadatas: Optional[List[types.Metadata]]
    documents: Optional[List[types.Document]]


# TODO: support arbitrary text everywhere so we don't SQL-inject ourselves.
# TODO: support empty strings everywhere
sql_alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./"
safe_text = st.text(alphabet=sql_alphabet, min_size=1)

safe_integers = st.integers(min_value=-2**31, max_value=2**31-1) # TODO: handle longs
safe_floats = st.floats(allow_infinity=False, allow_nan=False)   # TODO: handle infinity and NAN
safe_values = [safe_text, safe_integers, safe_floats]

float_types = [np.float16, np.float32, np.float64]
int_types = [np.int16, np.int32, np.int64]  # TODO: handle int types


@st.composite
def collection_name(draw) -> str:
    _collection_name_re = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{1,60}[a-zA-Z0-9]$")
    _ipv4_address_re = re.compile(r"^([0-9]{1,3}\.){3}[0-9]{1,3}$")
    _two_periods_re = re.compile(r"\.\.")

    name = draw(st.from_regex(_collection_name_re))
    hypothesis.assume(not _ipv4_address_re.match(name))
    hypothesis.assume(not _two_periods_re.search(name))

    return name


documents = st.lists(st.text(max_size=32),
                     min_size=2, max_size=10).map(lambda x: " ".join(x))

collection_metadata = st.one_of(st.none(),
                                st.dictionaries(safe_text, st.one_of(*safe_values)))

# TODO: Use a hypothesis strategy while maintaining embedding uniqueness
#       Or handle duplicate embeddings within a known epsilon
def create_embeddings(dim: int, count: int, dtype: np.dtype) -> types.Embeddings:
    return np.random.uniform(
        low=-1.0,
        high=1.0,
        size=(count, dim),
    ).astype(dtype).tolist()


@dataclass
class Collection():
    name: str
    metadata: Optional[types.Metadata]
    dimension: int
    dtype: np.dtype
    known_metadata_keys: Dict[str, st.SearchStrategy]
    has_documents: bool = False
    embedding_function: Optional[Callable[[str], types.Embedding]] = lambda x: []

@st.composite
def collections(draw):
    """Strategy to generate a Collection object"""

    name = draw(collection_name())
    metadata = draw(collection_metadata)
    dimension = draw(st.integers(min_value=2, max_value=2048))
    dtype = draw(st.sampled_from(float_types))

    known_metadata_keys = {}
    while len(known_metadata_keys) < 5:
        key = draw(safe_text)
        known_metadata_keys[key] = draw(st.sampled_from(safe_values))

    has_documents = draw(st.booleans())

    return Collection(name, metadata, dimension, dtype,
                      known_metadata_keys, has_documents)

@st.composite
def metadata(draw, collection: Collection):
    """Strategy for generating metadata that could be a part of the given collection"""
    md = draw(st.dictionaries(safe_text, st.one_of(*safe_values)))
    md.update(draw(st.fixed_dictionaries({}, optional=collection.known_metadata_keys)))
    return md


@st.composite
def record(draw,
           collection: Collection,
           id_strategy=safe_text):

    md = draw(metadata(collection))

    embeddings = create_embeddings(collection.dimension, 1, collection.dtype)

    if collection.has_documents:
        document = draw(documents)
    else:
        document = None

    return {"id": draw(id_strategy),
            "embedding": embeddings[0],
            "metadata": md,
            "document": document}


@st.composite
def recordsets(draw,
               collection_strategy=collections(),
               id_strategy=safe_text,
               min_size=1,
               max_size=50) -> RecordSet:

    collection = draw(collection_strategy)

    records = draw(st.lists(record(collection, id_strategy),
                            min_size=min_size, max_size=max_size))

    records = {r["id"]: r for r in records}.values()  # Remove duplicates

    return {
        "ids": [r["id"] for r in records],
        "embeddings": [r["embedding"] for r in records],
        "metadatas": [r["metadata"] for r in records],
        "documents": [r["document"] for r in records] if collection.has_documents else None,
    }


# This class is mostly cloned from from hypothesis.stateful.RuleStrategy,
# but always runs all the rules, instead of using a FeatureStrategy to
# enable/disable rules. Disabled rules cause the entire test to be marked invalida and,
# combined with the complexity of our other strategies, leads to an
# unacceptably increased incidence of hypothesis.errors.Unsatisfiable.
class DeterministicRuleStrategy(SearchStrategy):
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

        rule = data.draw(
            st.sampled_from([r for r in self.rules if self.is_valid(r)])
        )
        argdata = data.draw(rule.arguments_strategy)
        return (rule, argdata)

    def is_valid(self, rule):
        if not all(precond(self.machine) for precond in rule.preconditions):
            return False

        for b in rule.bundles:
            bundle = self.machine.bundle(b.name)
            if not bundle:
                return False
        return True


# See if there's a way to unify this so we're randomly generating keys
filterable_metadata = st.fixed_dictionaries({}, optional={"intKey": st.integers(max_value=2**31-1,
                                                                                min_value=-2**31-1),
                                                          "floatKey": st.floats(allow_infinity=False, allow_nan=False),
                                                          "textKey": st.text()})


# TODO remove hardcoded values
doc_tokens = ["apple", "grape", "peach", "cherry", "orange",
              "banana", "papaya", "plum", "mango", "melon"]

readable_document = st.lists(st.sampled_from(doc_tokens),
                             min_size=2,
                             max_size=10).map(lambda l: " ".join(l))

where_document_clause = st.sampled_from(doc_tokens).map(lambda t: {"$contains": t})

@st.composite
def where_clause(draw, int_values, float_values, text_values):
    key = draw(st.sampled_from(["intKey", "floatKey", "textKey"]))
    if key == "intKey":
        hypothesis.assume(len(int_values) > 0)
        value = draw(st.sampled_from(int_values))
    elif key == "floatKey":
        hypothesis.assume(len(float_values) > 0)
        value = draw(st.sampled_from(float_values))
    else:
        hypothesis.assume(len(text_values) > 0)
        value = draw(st.sampled_from(text_values))

    legal_ops = [None, "$eq", "$ne"]
    if key != "textKey":
        legal_ops = ["$gt", "$lt", "$lte", "$gte"] + legal_ops

    op = draw(st.sampled_from(legal_ops))

    if op is None:
        return {key: value}
    else:
        return {key: {op: value}}


@st.composite
def binary_operator_clause(draw, base_st):
    op = draw(st.sampled_from(["$and", "$or"]))
    return {op: [draw(base_st), draw(base_st)]}

@st.composite
def recursive_where_clause(draw, int_values, float_values, text_values):
    base_st = where_clause(int_values, float_values, text_values)
    return draw(st.recursive(base_st, binary_operator_clause))


recursive_where_document_clause = st.recursive(where_document_clause,
                                               binary_operator_clause)


@st.composite
def filterable_embedding_set(draw):

    def documents_st_fn(count):
        return st.lists(max_size=count, min_size=count,
                        elements=readable_document)

    def metadatas_st_fn(count):
        return st.lists(max_size=count, min_size=count,
                        elements=filterable_metadata)

    return draw(embedding_set(dimension=2,
                              documents_st_fn=documents_st_fn,
                              metadatas_st_fn=metadatas_st_fn))  # type: ignore


@st.composite
def filterable_embedding_set_with_filters(draw):

    es = draw(filterable_embedding_set())

    int_values = []
    float_values = []
    text_values = []
    for m in es["metadatas"]:
        if "intKey" in m:
            int_values.append(m["intKey"])
        if "floatKey" in m:
            float_values.append(m["floatKey"])
        if "textKey" in m:
            text_values.append(m["textKey"])

    size = len(es["ids"])

    filters = draw(st.lists(recursive_where_clause(int_values,
                                                   float_values,
                                                   text_values),
                            min_size=size, max_size=size))

    doc_filters = draw(st.lists(recursive_where_document_clause,
                                min_size=size, max_size=size))

    return es, filters, doc_filters
