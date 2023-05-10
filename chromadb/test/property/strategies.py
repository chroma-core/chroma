import hashlib
import hypothesis
import hypothesis.strategies as st
from typing import Optional, List, Dict, Union
from typing_extensions import TypedDict
import numpy as np
import chromadb.api.types as types
import re
from hypothesis.strategies._internal.strategies import SearchStrategy
from hypothesis.errors import InvalidDefinition

from dataclasses import dataclass

# Set the random seed for reproducibility
np.random.seed(0)  # unnecessary, hypothesis does this for us

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

test_hnsw_config = {
    "hnsw:construction_ef": 128,
    "hnsw:search_ef": 128,
    "hnsw:M": 128,
}


class RecordSet(TypedDict):
    """
    A generated set of embeddings, ids, metadatas, and documents that
    represent what a user would pass to the API.
    """

    ids: Union[types.ID, List[types.ID]]
    embeddings: Optional[Union[types.Embeddings, types.Embedding]]
    metadatas: Optional[Union[List[types.Metadata], types.Metadata]]
    documents: Optional[Union[List[types.Document], types.Document]]


# TODO: support arbitrary text everywhere so we don't SQL-inject ourselves.
# TODO: support empty strings everywhere
sql_alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
safe_text = st.text(alphabet=sql_alphabet, min_size=1)

# Workaround for FastAPI json encoding peculiarities
# https://github.com/tiangolo/fastapi/blob/8ac8d70d52bb0dd9eb55ba4e22d3e383943da05c/fastapi/encoders.py#L104
safe_text = safe_text.filter(lambda s: not s.startswith("_sa"))

safe_integers = st.integers(
    min_value=-(2**31), max_value=2**31 - 1
)  # TODO: handle longs
safe_floats = st.floats(
    allow_infinity=False, allow_nan=False, allow_subnormal=False
)  # TODO: handle infinity and NAN

safe_values = [safe_text, safe_integers, safe_floats]


def one_or_both(strategy_a, strategy_b):
    return st.one_of(
        st.tuples(strategy_a, strategy_b),
        st.tuples(strategy_a, st.none()),
        st.tuples(st.none(), strategy_b),
    )


# Temporarily generate only these to avoid SQL formatting issues.
legal_id_characters = (
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./+"
)

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


collection_metadata = st.one_of(
    st.none(), st.dictionaries(safe_text, st.one_of(*safe_values))
)


# TODO: Use a hypothesis strategy while maintaining embedding uniqueness
#       Or handle duplicate embeddings within a known epsilon
def create_embeddings(dim: int, count: int, dtype: np.dtype) -> types.Embeddings:
    return (
        np.random.uniform(
            low=-1.0,
            high=1.0,
            size=(count, dim),
        )
        .astype(dtype)
        .tolist()
    )


class hashing_embedding_function(types.EmbeddingFunction):
    def __init__(self, dim: int, dtype: np.dtype) -> None:
        self.dim = dim
        self.dtype = dtype

    def __call__(self, texts: types.Documents) -> types.Embeddings:
        # Hash the texts and convert to hex strings
        hashed_texts = [
            list(hashlib.sha256(text.encode("utf-8")).hexdigest()) for text in texts
        ]
        # Pad with repetition, or truncate the hex strings to the desired dimension
        padded_texts = [
            text * (self.dim // len(text)) + text[: self.dim % len(text)]
            for text in hashed_texts
        ]

        # Convert the hex strings to dtype
        return np.array(
            [[int(char, 16) / 15.0 for char in text] for text in padded_texts],
            dtype=self.dtype,
        ).tolist()


def embedding_function_strategy(
    dim: int, dtype: np.dtype
) -> st.SearchStrategy[types.EmbeddingFunction]:
    return st.just(hashing_embedding_function(dim, dtype))


@dataclass
class Collection:
    name: str
    metadata: Optional[types.Metadata]
    dimension: int
    dtype: np.dtype
    known_metadata_keys: Dict[str, st.SearchStrategy]
    known_document_keywords: List[str]
    has_documents: bool = False
    has_embeddings: bool = False
    embedding_function: Optional[types.EmbeddingFunction] = None


@st.composite
def collections(
    draw,
    add_filterable_data=False,
    with_hnsw_params=False,
    has_embeddings: Optional[bool] = None,
    has_documents: Optional[bool] = None,
) -> Collection:
    """Strategy to generate a Collection object. If add_filterable_data is True, then known_metadata_keys and known_document_keywords will be populated with consistent data."""

    assert not ((has_embeddings is False) and (has_documents is False))

    name = draw(collection_name())
    metadata = draw(collection_metadata)
    dimension = draw(st.integers(min_value=2, max_value=2048))
    dtype = draw(st.sampled_from(float_types))

    if with_hnsw_params:
        if metadata is None:
            metadata = {}
        metadata.update(test_hnsw_config)
        # Sometimes, select a space at random
        if draw(st.booleans()):
            # TODO: pull the distance functions from a source of truth that lives not
            # in tests once https://github.com/chroma-core/issues/issues/61 lands
            metadata["hnsw:space"] = draw(st.sampled_from(["cosine", "l2", "ip"]))

    known_metadata_keys = {}
    if add_filterable_data:
        while len(known_metadata_keys) < 5:
            key = draw(safe_text)
            known_metadata_keys[key] = draw(st.sampled_from(safe_values))

    if has_documents is None:
        has_documents = draw(st.booleans())
    if has_documents and add_filterable_data:
        known_document_keywords = draw(st.lists(safe_text, min_size=5, max_size=5))
    else:
        known_document_keywords = []

    if not has_documents:
        has_embeddings = True
    else:
        if has_embeddings is None:
            has_embeddings = draw(st.booleans())

    embedding_function = draw(embedding_function_strategy(dimension, dtype))

    return Collection(
        name=name,
        metadata=metadata,
        dimension=dimension,
        dtype=dtype,
        known_metadata_keys=known_metadata_keys,
        has_documents=has_documents,
        known_document_keywords=known_document_keywords,
        has_embeddings=has_embeddings,
        embedding_function=embedding_function,
    )


@st.composite
def metadata(draw, collection: Collection):
    """Strategy for generating metadata that could be a part of the given collection"""
    # First draw a random dictionary.
    md = draw(st.dictionaries(safe_text, st.one_of(*safe_values)))
    # Then, remove keys that overlap with the known keys for the coll
    # to avoid type errors when comparing.
    if collection.known_metadata_keys:
        for key in collection.known_metadata_keys.keys():
            if key in md:
                del md[key]
        # Finally, add in some of the known keys for the collection
        md.update(
            draw(st.fixed_dictionaries({}, optional=collection.known_metadata_keys))
        )
    return md


@st.composite
def document(draw, collection: Collection):
    """Strategy for generating documents that could be a part of the given collection"""

    if collection.known_document_keywords:
        known_words_st = st.sampled_from(collection.known_document_keywords)
    else:
        known_words_st = st.text(min_size=1)

    random_words_st = st.text(min_size=1)
    words = draw(st.lists(st.one_of(known_words_st, random_words_st), min_size=1))
    return " ".join(words)


@st.composite
def record(draw, collection: Collection, id_strategy=safe_text):
    md = draw(metadata(collection))

    if collection.has_embeddings:
        embedding = create_embeddings(collection.dimension, 1, collection.dtype)[0]
    else:
        embedding = None
    if collection.has_documents:
        doc = draw(document(collection))
    else:
        doc = None

    return {
        "id": draw(id_strategy),
        "embedding": embedding,
        "metadata": md,
        "document": doc,
    }


@st.composite
def recordsets(
    draw,
    collection_strategy=collections(),
    id_strategy=safe_text,
    min_size=1,
    max_size=50,
) -> RecordSet:
    collection = draw(collection_strategy)

    records = draw(
        st.lists(record(collection, id_strategy), min_size=min_size, max_size=max_size)
    )

    records = {r["id"]: r for r in records}.values()  # Remove duplicates

    ids = [r["id"] for r in records]
    embeddings = (
        [r["embedding"] for r in records] if collection.has_embeddings else None
    )
    metadatas = [r["metadata"] for r in records]
    documents = [r["document"] for r in records] if collection.has_documents else None

    # in the case where we have a single record, sometimes exercise
    # the code that handles individual values rather than lists
    if len(records) == 1:
        if draw(st.booleans()):
            ids = ids[0]
        if collection.has_embeddings and draw(st.booleans()):
            embeddings = embeddings[0]
        if draw(st.booleans()):
            metadatas = metadatas[0]
        if collection.has_documents and draw(st.booleans()):
            documents = documents[0]

    return {
        "ids": ids,
        "embeddings": embeddings,
        "metadatas": metadatas,
        "documents": documents,
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

        rule = data.draw(st.sampled_from([r for r in self.rules if self.is_valid(r)]))
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


@st.composite
def where_clause(draw, collection):
    """Generate a filter that could be used in a query against the given collection"""

    known_keys = sorted(collection.known_metadata_keys.keys())

    key = draw(st.sampled_from(known_keys))
    value = draw(collection.known_metadata_keys[key])

    legal_ops = [None, "$eq", "$ne"]
    if not isinstance(value, str):
        legal_ops = ["$gt", "$lt", "$lte", "$gte"] + legal_ops

    op = draw(st.sampled_from(legal_ops))

    if op is None:
        return {key: value}
    else:
        return {key: {op: value}}


@st.composite
def where_doc_clause(draw, collection):
    """Generate a where_document filter that could be used against the given collection"""
    if collection.known_document_keywords:
        word = draw(st.sampled_from(collection.known_document_keywords))
    else:
        word = draw(safe_text)
    return {"$contains": word}


@st.composite
def binary_operator_clause(draw, base_st):
    op = draw(st.sampled_from(["$and", "$or"]))
    return {op: [draw(base_st), draw(base_st)]}


@st.composite
def recursive_where_clause(draw, collection):
    base_st = where_clause(collection)
    return draw(st.recursive(base_st, binary_operator_clause))


@st.composite
def recursive_where_doc_clause(draw, collection):
    base_st = where_doc_clause(collection)
    return draw(st.recursive(base_st, binary_operator_clause))


class Filter(TypedDict):
    where: Optional[Dict[str, Union[str, int, float]]]
    ids: Optional[Union[str, List[str]]]
    where_document: Optional[types.WhereDocument]


@st.composite
def filters(
    draw,
    collection_st: st.SearchStrategy[Collection],
    recordset_st: st.SearchStrategy[RecordSet],
    include_all_ids=False,
) -> Filter:
    collection = draw(collection_st)
    recordset = draw(recordset_st)

    where_clause = draw(st.one_of(st.none(), recursive_where_clause(collection)))
    where_document_clause = draw(
        st.one_of(st.none(), recursive_where_doc_clause(collection))
    )

    ids = recordset["ids"]
    # Record sets can be a value instead of a list of values if there is only one record
    if isinstance(ids, str):
        ids = [ids]

    if not include_all_ids:
        ids = draw(st.one_of(st.none(), st.lists(st.sampled_from(ids))))
        if ids is not None:
            # Remove duplicates since hypothesis samples with replacement
            ids = list(set(ids))

    # Test both the single value list and the unwrapped single value case
    if ids is not None and len(ids) == 1 and draw(st.booleans()):
        ids = ids[0]

    return {"where": where_clause, "where_document": where_document_clause, "ids": ids}
