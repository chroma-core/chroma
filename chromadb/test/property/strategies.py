import hypothesis
import hypothesis.strategies as st
from typing import Optional, TypedDict, Callable, List, cast
import hypothesis.extra.numpy as npst
import numpy as np
import chromadb.api.types as types
import chromadb.utils.embedding_functions as embedding_functions
import re

# Set the random seed for reproducibility
np.random.seed(0)

# See Hypothesis documentation for creating strategies at
# https://hypothesis.readthedocs.io/en/latest/data.html

collection_metadata = st.one_of(
    st.none(),
    st.dictionaries(
        st.text(),
        st.one_of(
            st.text(), st.integers(), st.floats(allow_infinity=False, allow_nan=False)
        ),
    ),
)

# TODO: build a strategy that constructs english sentences instead of gibberish strings

document = st.from_type(Optional[str])

_collection_name_re = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{1,60}[a-zA-Z0-9]$")
_ipv4_address_re = re.compile(r"^([0-9]{1,3}\.){3}[0-9]{1,3}$")
_two_periods_re = re.compile(r"\.\.")


class EmbeddingSet(TypedDict):
    """
    An Embedding Set is a generated set of embeddings, ids, metadatas, and documents
     that represent what a user would pass to the API.
    """

    ids: types.IDs
    embeddings: Optional[types.Embeddings]

    # TODO: We should be able to handle None values
    metadatas: Optional[List[types.Metadata]]
    documents: Optional[List[types.Document]]


class Collection(TypedDict):
    name: str
    metadata: Optional[types.Metadata]


@st.composite
def collection_name(draw) -> Collection:
    """Strategy to generate a set of collections"""

    # name = draw(st.from_regex(coll_name_re))
    name = draw(st.one_of(st.from_regex(_collection_name_re)))
    hypothesis.assume(not _ipv4_address_re.match(name))
    hypothesis.assume(not _two_periods_re.search(name))
    return name


@st.composite
def collections(draw) -> Collection:
    """Strategy to generate a set of collections"""
    return {"name": draw(collection_name()), "metadata": draw(collection_metadata)}


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
int_types = [np.int16, np.int32, np.int64]

# TODO: Handle single embedding, metadata, and document i.e. not list


def embeddings_strategy(dim: int, count: int, dtype: np.dtype):
    return npst.arrays(
        dtype=dtype,
        shape=(count, dim),
        # TODO: It should be possible to deal with NaN and inf values
        # TODO: It should be possible to deal with redundant embeddings
        elements=st.floats(
            allow_nan=False,
            allow_infinity=False,
            width=np.dtype(dtype).itemsize * 8,
        )
        if dtype in float_types
        else st.integers(min_value=np.iinfo(dtype).min, max_value=np.iinfo(dtype).max),
        unique=True,
    )


# TODO: Use a hypothesis strategy while maintaining embedding uniqueness
#       Or handle duplicate embeddings within a known epsilon
def create_embeddings(dim: int, count: int, dtype: np.dtype):
    return np.random.uniform(
        low=-1.0,
        high=1.0,
        size=(count, dim),
    ).astype(dtype)


def documents_strategy(count: int) -> st.SearchStrategy[Optional[List[str]]]:
    # TODO: Handle non-unique documents
    # TODO: Handle empty string documents
    return st.one_of(
        st.none(),
        st.lists(st.text(min_size=1), min_size=count, max_size=count, unique=True),
    )


def metadata_strategy():
    # TODO: Handle NaN and inf values
    # TODO: Handle empty string keys
    return st.dictionaries(
        st.text(min_size=1),
        st.one_of(
            st.text(), st.integers(), st.floats(allow_infinity=False, allow_nan=False)
        ),
    )


def metadatas_strategy(count: int) -> st.SearchStrategy[Optional[List[types.Metadata]]]:
    return st.one_of(
        st.none(), st.lists(metadata_strategy(), min_size=count, max_size=count)
    )


@st.composite
def embedding_set(
    draw,
    dimension_st: st.SearchStrategy[int] = st.integers(min_value=2, max_value=2048),
    count_st: st.SearchStrategy[int] = st.integers(min_value=1, max_value=512),
    dtype_st: st.SearchStrategy[np.dtype] = st.sampled_from(float_types),
    id_st: st.SearchStrategy[str] = st.text(
        alphabet=legal_id_characters, min_size=1, max_size=64
    ),
    documents_st_fn: Callable[
        [int], st.SearchStrategy[Optional[List[str]]]
    ] = documents_strategy,
    metadatas_st_fn: Callable[
        [int], st.SearchStrategy[Optional[List[types.Metadata]]]
    ] = metadatas_strategy,
    dimension: Optional[int] = None,
    count: Optional[int] = None,
    dtype: Optional[np.dtype] = None,
) -> EmbeddingSet:
    """Strategy to generate a set of embeddings."""

    if count is None:
        count = draw(count_st)

    if dimension is None:
        dimension = draw(dimension_st)

    if dtype is None:
        # TODO Support integer types?
        dtype = draw(dtype_st)

    count = cast(int, count)
    dimension = cast(int, dimension)

    # TODO: Test documents only
    # TODO: Generative embedding function to guarantee unique embeddings for unique documents
    documents = draw(documents_st_fn(count))
    metadatas = draw(metadatas_st_fn(count))

    embeddings = create_embeddings(dimension, count, dtype)

    ids = set()
    while len(ids) < count:
        ids.add(draw(id_st))
    ids = list(ids)

    return {
        "ids": ids,
        "embeddings": embeddings.tolist() if embeddings is not None else None,
        "metadatas": metadatas,
        "documents": documents,
    }
