from typing import Any, List, Dict
from typing_extensions import Protocol, runtime_checkable
from chromadb.api.types import D, Embeddings
from enum import Enum
from abc import abstractmethod
import sys


class Space(Enum):
    COSINE = "cosine"
    L2 = "l2"
    INNER_PRODUCT = "inner_product"


@runtime_checkable
class EmbeddingFunction(Protocol[D]):
    @abstractmethod
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    @abstractmethod
    def __call__(self, input: D) -> Embeddings:
        ...

    @abstractmethod
    def name(self) -> str:
        ...

    def default_space(self) -> Space:
        return Space.COSINE

    def supported_spaces(self) -> List[Space]:
        return [Space.COSINE, Space.L2, Space.INNER_PRODUCT]

    def max_tokens(self) -> int:
        return sys.maxsize

    @abstractmethod
    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[D]":
        ...

    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        ...

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        pass

    def validate_config(self, config: Dict[str, Any]) -> None:
        pass
