import hypothesis
import hypothesis.strategies as st
from typing import Dict, Optional, Sequence, Tuple, TypedDict, cast
import hypothesis.extra.numpy as npst
import numpy.typing as npt
import numpy as np
import chromadb.api.types as types
import re

# See Hypothesis documentation for creating strategies at
# https://hypothesis.readthedocs.io/en/latest/data.html

metadata = st.from_type(types.Metadata)
collection_metadata = st.from_type(Optional[types.Metadata])

# TODO: build a strategy that constructs english sentences instead of gibberish strings

# TODO: collection names should be arbitrary strings
# _collection_name_re = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]$")
_collection_name_re = re.compile(r"^[a-z0-9][a-z0-9._-][a-z0-9]$")
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


def unique_ids_strategy(count: int):
    # TODO: Handle non-unique ids
    return st.lists(st.text(), min_size=count, max_size=count, unique=True)


float_types = [np.float16, np.float32, np.float64]
int_types = [np.int16, np.int32, np.int64]


def embeddings_strategy(dim: int, count: int, dtype: np.dtype):
    return npst.arrays(
        dtype=dtype,
        shape=(count, dim),
        # TODO: It should be possible to deal with NaN and inf values
        elements=st.floats(
            allow_nan=False, allow_infinity=False, width=np.dtype(dtype).itemsize * 8
        )
        if dtype in float_types
        else st.integers(min_value=np.iinfo(dtype).min, max_value=np.iinfo(dtype).max),
    )


def documents_strategy(count: int):
    return st.lists(st.text(), min_size=count, max_size=count)


def metadatas_strategy(count: int):
    return st.one_of(st.lists(metadata, min_size=count, max_size=count), st.none())


@st.composite
def embedding_set(
    draw,
    dimension: Optional[int] = None,
    count: Optional[int] = None,
    dtype: Optional[np.dtype] = None,
) -> EmbeddingSet:
    """Strategy to generate a set of embeddings."""

    if dimension is None:
        dimension = draw(st.integers(min_value=1, max_value=2048))

    if count is None:
        count = draw(st.integers(min_value=1, max_value=2000))

    if dtype is None:
        dtype = draw(
            st.sampled_from([np.float16, np.float32, np.float64, np.int16, np.int32, np.int64])
        )

    count = cast(int, count)
    dimension = cast(int, dimension)

    # TODO: should be possible to deal with empty sets
    ids = draw(st.lists(st.text(), min_size=count, max_size=count))

    embeddings, documents = draw(
        one_or_both(embeddings_strategy(dimension, count, dtype), documents_strategy(count))
    )

    metadatas = draw(st.lists(metadata, min_size=count, max_size=count))

    return {
        "ids": ids,
        "embeddings": embeddings.tolist() if embeddings is not None else None,
        "metadatas": metadatas,
        "documents": documents,
    }
