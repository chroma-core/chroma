import hypothesis
import hypothesis.strategies as st
from typing import Optional, TypedDict, Callable, List, Dict, Union, cast
import hypothesis.extra.numpy as npst
import numpy as np
import chromadb.api.types as types
import chromadb.utils.embedding_functions as embedding_functions
import re

# Set the random seed for reproducibility
np.random.seed(0) # unnecessary, hypothesis does this for us

# See Hypothesis documentation for creating strategies at
# https://hypothesis.readthedocs.io/en/latest/data.html


class RecordSet(TypedDict):
    """
    A generated set of embeddings, ids, metadatas, and documents that
    represent what a user would pass to the API.
    """
    ids: types.IDs
    embeddings: Optional[types.Embeddings]
    metadatas: Optional[List[types.Metadata]]
    documents: Optional[List[types.Document]]


@st.composite
def collection_name(draw) -> str:

    _collection_name_re = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{1,60}[a-zA-Z0-9]$")
    _ipv4_address_re = re.compile(r"^([0-9]{1,3}\.){3}[0-9]{1,3}$")
    _two_periods_re = re.compile(r"\.\.")

    name = draw(st.from_regex(_collection_name_re))
    hypothesis.assume(not _ipv4_address_re.match(name))
    hypothesis.assume(not _two_periods_re.search(name))

    return name


# TODO: support arbitrary text everywhere so we don't SQL-inject ourselves.
# TODO: support empty strings everywhere
sql_alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./"
safe_text = st.text(alphabet=sql_alphabet, min_size=1)
#safe_text = st.uuids().map(lambda x: str(x))

safe_integers = st.integers(min_value=-2**31, max_value=2**31-1) # TODO: handle longs
safe_floats = st.floats(allow_infinity=False, allow_nan=False)   # TODO: handle infinity and NAN
safe_values = [safe_text, safe_integers, safe_floats]

float_types = [np.float16, np.float32, np.float64]
int_types = [np.int16, np.int32, np.int64] # TODO: handle int types


documents = st.lists(safe_text, min_size=2, max_size=10).map(lambda x: " ".join(x))

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


class Collection():
    name: str
    metadata: Optional[types.Metadata]
    dimension: int
    dtype: np.dtype
    known_metadata_keys: Dict[str, st.SearchStrategy]

    def __init__(self,
                 name: str,
                 metadata: Optional[Optional[types.Metadata]],
                 dimension: int,
                 dtype: np.dtype,
                 known_metadata_keys: Dict[str, st.SearchStrategy],
                 has_documents: bool) -> None:
        self.name = name
        self.metadata = metadata
        self.dimension = dimension
        self.dtype = dtype
        self.known_metadata_keys = known_metadata_keys
        self.has_documents = has_documents
        self.ef = lambda x: None


@st.composite
def collections(draw):
    """Strategy to generate a Collection object"""

    name = draw(collection_name())
    metadata = draw(collection_metadata)
    dimension = draw(st.integers(min_value=2, max_value=2048))
    dtype = draw(st.sampled_from(float_types))

    known_metadata_keys = draw(st.dictionaries(safe_text,
                                               st.sampled_from([*safe_values]),
                                               min_size=5))

    has_documents = draw(st.booleans())

    return Collection(name, metadata, dimension, dtype,
                      known_metadata_keys, has_documents)

@st.composite
def metadata(draw, collection: Collection):
    """Strategy for generating metadata that could be a part of the given collection"""

    random_metadata_st = st.dictionaries(safe_text, st.one_of(*safe_values))
    known_metadata_st = st.fixed_dictionaries(mapping={},
                                              optional=collection.known_metadata_keys)
    metadata_st = _dict_merge(random_metadata_st, known_metadata_st)

    return draw(st.one_of(st.none(), metadata_st))


@st.composite
def record(draw,
           collection: Collection,
           id_strategy=safe_text):

    embeddings = create_embeddings(collection.dimension, 1, collection.dtype)

    if collection.has_documents:
        document = draw(documents)
    else:
        document = None

    return {"id": draw(id_strategy),
            "embedding": embeddings[0],
            "metadata": draw(metadata(collection)),
            "document": document}


# Reecordsets, but draws by row instead of by column
@st.composite
def recordsets(draw,
               collection_strategy=collections(),
               id_strategy=safe_text,
               min_size=1,
               max_size=50) -> RecordSet:

    collection = draw(collection_strategy)

    records = draw(st.lists(record(collection, id_strategy),
                            min_size=min_size, max_size=max_size))

    ids = [r["id"] for r in records]
    embeddings = [r["embedding"] for r in records]
    metadatas = [r["metadata"] for r in records]
    docs = [r["document"] for r in records]

    return {
        "ids": ids,
        "embeddings": embeddings,
        "metadatas": metadatas,
        "documents": docs if collection.has_documents else None
    }


@st.composite
def _dict_merge(draw, *strategies: st.SearchStrategy[Dict]) -> Dict:
    """Strategy to merge the results of multiple strategies that return dicts into a single dict"""
    result = {}
    for strategy in strategies:
        result.update(draw(strategy))
    return result