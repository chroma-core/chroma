# CIP-3: Developer Experience - Max Embedding Function Token Input

## Status

Current Status: `Draft`

## Motivation

> Note: This CIP was inspired
>
by [[Feature Request]: easy chunking, and token limit based retrieval](https://github.com/chroma-core/chroma/issues/430)

As many new users adopt Chroma, it is fair to assume that a part of the developer population will not be too familiar of
the concept of max input tokens of a model. In this CIP we propose that we introduce a mechanics that will warn or even
prevent users from using models with too high max input tokens.

### Suggested Changes

We propose that we introduce a new abstract method in the `EmbeddingFunction` interface:

```python
class EmbeddingFunction(Protocol):
    def max_input_length(self) -> int:
        ...
```

The purpose of the method is to enforce each existing and new Embedding Function to implement a method that will return
the underlying model's max input length (max tokens).

We also propose that there are two modes of operations:

- `warning` - if the input length is greater than the max input length of the model, a warning is logged
- `exception` - if the input length is greater than the max input length of the model, an exception is raised

The `warning` mode will be enabled by default. For `exception` mode we suggest to introduce a new environment variable
`CHROMA_STRICT_MODE` with possible values `true` and `false` (default). When `true` is specified an exception will be
raised if the input length is greater than the max input length of the model.

### Challenges

There are several challenges:

- Given the multitude of models we need to find uniform way to calculate the number of tokens for the documents.
- For some models like OpenAI, Cohere, PaLM max input tokens are fixed, however for other models things are not so black
  and white and will require some additional consideration as to how to get the max input tokens size.

## Compatibility, Deprecation, and Migration Plan

In terms for compatibility we do not introduce breaking changes. Implementing the `max_input_length` method is optional
and for some EFs it will be a no-op.

## Test Plan

Additional tests are added to cover warning an exception cases.

Tests will be stored under `chromadb/tests/ef/test_ef_exceptions.py`

