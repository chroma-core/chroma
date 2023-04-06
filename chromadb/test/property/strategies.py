import hypothesis
import hypothesis.strategies as st
from typing import Dict, Optional, Sequence, TypedDict, cast
import hypothesis.extra.numpy as npst
import numpy.typing as npt
import numpy as np
import chromadb.api.types as types
import re

# See Hypothesis documentation for creating strategies at
# https://hypothesis.readthedocs.io/en/latest/data.html

metadata = st.from_type(Optional[types.Metadata])

# TODO: build a strategy that constructs english sentences instead of gibberish strings
# Unsure what would happen feeding random unicode to an embedding model, could get bad results

document = st.from_type(Optional[str])

_coll_name_re = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]$")
_ipv4_address_re = re.compile(r"^([0-9]{1,3}\.){3}[0-9]{1,3}$")
_two_periods_re = re.compile(r"\.\.")


class EmbeddingSet(TypedDict):
    ids: types.IDs
    embeddings: types.Embeddings
    metadatas: Optional[Sequence[Optional[types.Metadata]]]
    documents: Optional[Sequence[Optional[types.Metadata]]]


class Collection(TypedDict):
    name: str
    metadata: Optional[types.Metadata]


@st.composite
def collections(draw) -> Collection:
    """Strategy to generate a set of collections"""

    # name = draw(st.from_regex(coll_name_re))
    name = draw(st.one_of(st.from_regex(_coll_name_re)))
    hypothesis.assume(not _ipv4_address_re.match(name))
    hypothesis.assume(not _two_periods_re.search(name))

    return {"name": name, "metadata": draw(metadata)}


@st.composite
def embeddings(
    draw,
    dimension: Optional[int] = None,
    count: Optional[int] = None,
    dtype: Optional[np.dtype] = None,
) -> EmbeddingSet:
    """Strategy to generate a set of embeddings."""

    if dimension is None:
        dimension = draw(st.integers(min_value=1, max_value=2048))

    if count is None:
        count = draw(st.integers(min_value=0, max_value=2000))

    if dtype is None:
        dtype = draw(
            st.sampled_from(
                [np.float16, np.float32, np.float64, np.int16, np.int32, np.int64]
            )
        )

    count = cast(int, count)
    dimension = cast(int, dimension)

    vectors = draw(npst.arrays(dtype=dtype, shape=(dimension, count)))
    ids = draw(st.lists(st.text(), min_size=count, max_size=count))
    metadatas = draw(st.lists(metadata, min_size=count, max_size=count))
    documents = draw(st.lists(st.text(), min_size=count, max_size=count))

    return {
        "ids": ids,
        "embeddings": vectors.tolist(),
        "metadatas": metadatas,
        "documents": documents,
    }
