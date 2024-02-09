# CIP-02082024 Rankers

## Motivation

Hybrid search has proven to be a great technique for improving the relevancy of search results.

The concept of rankers is to create a simple yet powerful abstraction that both Chroma and community can build upon. The
ranker functions are designed to be a way to produce a score for a given document and query. Fusion of the scores
is out of scope for this CIP.

We propose that the ranker functions are implemented in both server and/or client. It is important to observe that not
all ranker implementations are suitable for server-side. For example, a ranker that requires a large model to be loaded
in memory is not suitable for server-side.

## Public Interfaces

The following public interfaces are proposed:

- Change in the `query` method of the `Collection` class to accept a `ranker` parameter. The `ranker` parameter is a
  string that specifies the ranker function to be used. The ranker function is a simple function that takes a query and
  a document and returns a score. The `ranker` parameter is optional and if not provided, the default ranker function
  is used.
- Changes to `chromadb.api.types.QueryResult` to add an optional ranker score field. The field should contain the id of
  te ranker function and the produced score. Alternatively, a new subclass with extended attributes can be created
  e.g. `RankerQueryResult` (preferred option).

### Ranker Functions

We suggest that ranker functions follow similar, if not same signature as embedding functions:

```python
from typing import Union, TypeVar
from typing_extensions import Protocol
from chromadb.api.types import RankerQueryResult, QueryResult

Rankable = Union[str, int, QueryResult]
R = TypeVar('R', bound=Rankable, contravariant=True)


class RankerFunction(Protocol[R]):
    def __call__(self, results: R) -> RankerQueryResult:
        ...
```

## Proposed Changes

### Initial implementations

We suggest that the following three ranker functions are implemented as a starting point:

- `bm25` - A simple ranker that uses single-node Chroma's SQLite built-in BM25 implementation.
- `cohere` - A ranker that uses Cohere's rerank models endpoint.
- `sentence-transformers` - A ranker that uses Sentence Transformers to produce a score. In particular, we suggest a
  sample implementation that uses [`BAAI/bge-m3`](https://huggingface.co/BAAI/bge-m3) with combined scores.

### References

- https://arxiv.org/pdf/2210.11934.pdf
- https://www.pinecone.io/blog/hybrid-search/
- https://qdrant.tech/articles/hybrid-search/#
- https://neptune.ai/blog/recommender-systems-metrics
- https://txt.cohere.com/rerank/
- https://huggingface.co/BAAI/bge-m3

#### Future Work

As a quick follow to this CIP and related implementation, we suggest the introduction of Fusion Functions which can take
the output of one or more rankers along with HNSW distance metric and merge them into a single score.

We believe that the ranker functions will play nicely with upcoming pipelines feature.

## Compatibility, Deprecation, and Migration Plan

From API perspective, the changes are backward compatible. The functionality itself will not be backward compatible.

## Test Plan

API tests with SSL verification enabled and a self-signed certificate.

## Rejected Alternatives

N/A