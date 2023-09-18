# CIP-5: Feedback Endpoint

## Status

Current Status: `Under Discussion`

## **Motivation**

Embeddings-based retrieval accuracy can be improved both by fine-tuning the embedding model,
and by fitting an affine transform which can be applied to the query vector. In order to support this
use-case, we need to be able to collect a feedback signal regarding the relevance of the retrieved
items, perform the fine tuning / fit the transform, and then use the updated model / transform for the collection.

This CIP proposes to add a feedback endpoint to Chroma to enable tuning the embedding space.

## **Public Interfaces**

An endpoint on `Collection` to collect feedback:

```python
def add_feedback(
    self,
    feedbacks: Feedbacks,
) -> None:
```

Where the `Feedbacks` type is a list of `Feedback` objects composed of Chroma types:

```python
class Feedback:
    queries: Sequence[Union[Embedding, Document]]
    results: Sequence[QueryResult]
    signal: FeedbackSignal # Enum('FeedbackSignal', ['Positive', 'Negative'])
```

An endpoint to trigger tuning:

```python
def tune(
  self,
  feedbacks: Optional[Feedbacks],
) -> None:
```

When tuning is complete, the collection's `embedding_function` will be updated to reflect the new embedding function.

## **Proposed Changes**

Introducing the capabiltiy to tune the embedding space with respect to human feedback reqiures several components.

- An API endpoint to collect feedback signal. We propose adding this endpoint on a collection, since there is a 1:1 relationship between collection and embedding model.
- A way to store feedback such that it can be accessed by the tuning process.

Initially we propose creating a 'feedback' collection for each collection which has feedback. The feedback collection will be created automatically the first time `add_feedback` is called on a collection.

- Queries and results are stored in the same feedback collection, separated and associated via metadata.
- The feedback collection itself is associated with the query collection via collection metadata.

```

```
