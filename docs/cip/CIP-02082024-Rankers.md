# CIP-02082024 Ranking Functions (Rf)

## Motivation

Hybrid search has proven to be a great technique for improving the relevancy of search results.

The concept of Ranking Functions is to create a simple yet powerful abstraction that both Chroma and community can build
upon. The Ranking Functions (Rf) are designed to be a way to produce a score for a given document and query. Fusion of
the scores is out of scope for this CIP.

We propose that the Ranking Functions are implemented in both server and/or client. It is important to observe that not
all Rf implementations are suitable for server-side. For example, a Ranking Function (Rf) that requires a large
model to be loaded
in memory is not suitable for server-side.

## Public Interfaces

The following public interfaces are proposed:

- Change in the `query` method of the `Collection` class to accept a `ranking_functions` parameter.
  The `ranking_functions` parameter is a
  list of serializable objects that represent the identity of a Rf and any additional configuration params. The Ranking
  Function is a simple function that takes a query and a document and returns a score. The `ranking_functions` parameter
  is optional and if not provided, the default Ranking Function is used.
- Changes to `chromadb.api.types.QueryResult` to add an optional `RankingScore` field. The field should contain the id
  of te Ranking Function and the produced score. Alternatively, a new subclass with extended attributes can be created
  e.g. `RankerQueryResult` (preferred option).

### Ranking Functions

We suggest that Ranking Functions follow similar, if not same signature as embedding functions:

```python
from typing import Union, TypeVar, TypedDict, Optional, List
from typing_extensions import Protocol
from chromadb.api.types import QueryResult

Rank = Union[int, float]


class RankingScore(TypedDict):
    rf_id: str
    rank: Rank


class RankedQueryResult(QueryResult):
    ranks: Optional[List[List[RankingScore]]]


Rankable = Union[str, int, QueryResult]

R = TypeVar("R", bound=Rankable, contravariant=True)


class RankingFunction(Protocol[R]):
    def get_id(self) -> str:
        ...

    def __call__(self, results: R) -> RankedQueryResult:
        ...

```

## Proposed Changes

Common Ranking Function constraints:

- Ranking functions must be thread-safe.

### Server-side Ranking Functions

To introduce ranking functions on the server-side, we consider the following:

- To take full advantage of the Chroma server (both single-node and distributed API server), we propose that the ranking
  functions inherit from `Component` class. This will allow the ranking function to follow standard Chroma server
  lifecycle events for loading/starting/stopping.
- Allowed Rf should be defined as configuration.
- We suggest Ranking function loading to be lazy, i.e. the ranking function is loaded only when it is used.
- Ranking functions are loaded and used as close to the data as possible to avoid back and forth data transfer.
    - For single-node Chroma, we suggest that ranking functions are tail-loaded within `SegmentAPI` in query method.
    - For distributed Chroma - TBD (Separate CIP?)

### Initial implementations

We suggest that the following three Ranking Functions are implemented as a starting point:

- `bm25` - Rf that uses single-node Chroma's SQLite built-in BM25 implementation.
- `cohere` - Rf that uses Cohere's rerank models endpoint. (this in itself can be a whole class of Rfs that integrate
  with external services)
- `sentence-transformers` - Rf that uses Sentence Transformers to produce a score. In particular, we suggest a
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
the output of one or more Rfs along with HNSW distance metric and merge them into a single score.

We believe that the Ranking functions will play nicely with upcoming pipelines feature.

## Compatibility, Deprecation, and Migration Plan

From API perspective, the changes are backward compatible. The functionality itself will not be backward compatible.

## Test Plan

API tests with SSL verification enabled and a self-signed certificate.

## Rejected Alternatives

N/A