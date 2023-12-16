from __future__ import annotations
from chromadb.api.types import (
    Documents,
    Images,
    EmbeddingFunction,
    Embeddings,
)

from typing import Type, Union, Callable, Any
from tenacity import retry, stop_never, wait_exponential_jitter

EmbeddingFuncType = Type[EmbeddingFunction[Any]]
EmbeddingFunctionCall = Callable[
    [EmbeddingFuncType, Union[Documents, Images]], Embeddings
]


# 1st level used to access the class attributes
# wait parameters are taken from self
# 2nd level is the retried call
def retry_decorator(call: EmbeddingFunctionCall) -> EmbeddingFunctionCall:
    def decorator(
        cls: EmbeddingFuncType, input: Union[Documents, Images]
    ) -> EmbeddingFunctionCall:
        stop = getattr(cls, "stop", stop_never)
        wait = getattr(cls, "wait", wait_exponential_jitter())

        @retry(stop=stop, wait=wait, reraise=True)
        def wrapped_call(
            cls: EmbeddingFuncType, input: Union[Documents, Images]
        ) -> Embeddings:
            return call(cls, input)

        return wrapped_call

    return decorator
