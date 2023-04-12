import hypothesis
import hypothesis.strategies as st
from typing import Optional, Sequence, TypedDict, cast
import hypothesis.extra.numpy as npst
import numpy as np
import chromadb.api.types as types
import re

# Set the random seed for reproducibility
np.random.seed(0)

# See Hypothesis documentation for creating strategies at
# https://hypothesis.readthedocs.io/en/latest/data.html

collection_metadata = st.from_type(Optional[types.Metadata])

# TODO: build a strategy that constructs english sentences instead of gibberish strings

# TODO: collection names should be arbitrary strings
# _collection_name_re = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]$")
_collection_name_re = re.compile(r"^[a-z0-9][a-z0-9._-]{1,60}[a-z0-9]$")
_ipv4_address_re = re.compile(r"^([0-9]{1,3}\.){3}[0-9]{1,3}$")
_two_periods_re = re.compile(r"\.\.")


class EmbeddingSet(TypedDict):
    ids: types.IDs
    embeddings: Optional[types.Embeddings]

    # TODO: We should be able to handle None values
    metadatas: Optional[Sequence[types.Metadata]]
    documents: Optional[Sequence[types.Document]]


class Collection(TypedDict):
    name: str
    metadata: Optional[types.Metadata]


@st.composite
def collections(draw) -> Collection:
    """Strategy to generate a set of collections"""

    # name = draw(st.from_regex(coll_name_re))
    name = draw(st.one_of(st.from_regex(_collection_name_re)))
    hypothesis.assume(not _ipv4_address_re.match(name))
    hypothesis.assume(not _two_periods_re.search(name))

    return {"name": name, "metadata": draw(collection_metadata)}


def one_or_both(strategy_a, strategy_b):
    return st.one_of(
        st.tuples(strategy_a, strategy_b),
        st.tuples(strategy_a, st.none()),
        st.tuples(st.none(), strategy_b),
    )


@st.composite
def unique_ids_strategy(draw, count: int):

    ratio = 20
    strs = count // ratio

    str_results = draw(
        st.lists(st.text(min_size=1, max_size=64), min_size=strs, max_size=strs, unique=True)
    )

    # Rotate selections from between the two lists. This is a workaround for making sure we don't try to generate
    # too many strings, causing the Hypothesis health check to fail.ß
    results = []
    for i in range(count):
        if i % ratio == 0 and len(str_results) > 0:
            results.append(str_results.pop())
        else:
            results.append(str(draw(st.uuids())))

    return results


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


def documents_strategy(count: int):
    # TODO: Handle non-unique documents
    # TODO: Handle empty string documents
    return st.one_of(
        st.lists(st.text(min_size=1), min_size=count, max_size=count, unique=True), st.none()
    )


def metadata_strategy():
    # TODO: Handle NaN and inf values
    # TODO: Handle empty string keys
    return st.dictionaries(
        st.text(min_size=1),
        st.one_of(st.text(), st.integers(), st.floats(allow_infinity=False, allow_nan=False)),
    )


def metadatas_strategy(count: int):
    return st.one_of(
        st.lists(metadata_strategy(), min_size=count, max_size=count),
        st.none(),
    )


@st.composite
def embedding_set(
    draw,
    dimension: Optional[int] = None,
    count: Optional[int] = None,
    dtype: Optional[np.dtype] = None,
) -> EmbeddingSet:
    """Strategy to generate a set of embeddings."""

    if count is None:
        count = draw(st.integers(min_value=1, max_value=512))

    if dimension is None:
        dimension = draw(st.integers(min_value=2, max_value=2048))

    if dtype is None:
        # TODO Support integer types?
        dtype = draw(st.sampled_from(float_types))

    count = cast(int, count)
    dimension = cast(int, dimension)

    # TODO: should be possible to deal with empty sets
    ids = draw(unique_ids_strategy(count))

    # TODO: Test documents only
    # TODO: Generative embedding function to guarantee unique embeddings for unique documents
    documents = draw(documents_strategy(count))
    metadatas = draw(metadatas_strategy(count))
    embeddings = create_embeddings(dimension, count, dtype)

    return {
        "ids": ids,
        "embeddings": embeddings.tolist() if embeddings is not None else None,
        "metadatas": metadatas,
        "documents": documents,
    }
