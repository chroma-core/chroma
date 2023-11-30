# CIP-11302023: Developer Experience - Max Token Input

## Status

Current Status: `Under Discussion`

## Motivation

> Note: This CIP was inspired
> by [[Feature Request]: easy chunking, and token limit based retrieval](https://github.com/chroma-core/chroma/issues/430)

As many new users adopt Chroma, it is fair to assume that a part of the developer population will not be too familiar of
the concept of max input tokens of a model. In this CIP we propose that we introduce a mechanics that will warn or even
prevent users from using models with too high max input tokens.

### Suggested Changes

We propose that we introduce a new abstract method in the `EmbeddingFunction` interface:

```python
class EmbeddingFunction(Protocol):
    def max_input_length(self) -> int:
        return -1
```

The purpose of the method is to allow each existing and new Embedding Function to implement a method that will return
the underlying model's max input sequence (max tokens). The max input sequence can be a fixed number for models like 
OpenAI, Cohere, PaLM etc, for other models like Sentence Transformers, Universal Sentence Encoder, etc. the max input
sequence is taken from the model's tokenizer. By default, the method's implementation will return -1 which will indicate
that the EF does not support max input sequence.

We propose that the user is only warned when the underlying implementation is 100% sure about the max input sequence.
In all other situations no feedback is given to the user.

> **IMPORTANT**: The mechanics introduced by this CIP shall not cause any embedding function to break.

## Compatibility, Deprecation, and Migration Plan

In terms for compatibility we do not introduce breaking changes. Implementing the `max_input_length` method is optional
and for some EFs it will be a no-op.

Cross client-server compatability is also not impacted as the changes to the Embedding Function interface are purely on
the client side.

## Test Plan

Additional tests are added to cover warning an exception cases.

Tests will be stored under `chromadb/tests/ef/test_ef_exceptions.py`
