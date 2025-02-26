from typing import Any, List, Dict
from typing_extensions import Protocol, runtime_checkable
from chromadb.api.types import D, Embeddings
from enum import Enum
from abc import abstractmethod


class Space(Enum):
    COSINE = "cosine"
    L2 = "l2"
    INNER_PRODUCT = "inner_product"


@runtime_checkable
class EmbeddingFunction(Protocol[D]):
    """
    A protocol for embedding functions. To implement a new embedding function,
    you need to implement the following methods at minimum:
    - __init__
    - __call__
    - name
    - build_from_config
    - get_config
    """

    @abstractmethod
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the embedding function.
        Pass any arguments that will be needed to build the embedding function
        config.
        """
        ...

    @abstractmethod
    def __call__(self, input: D) -> Embeddings:
        ...

    @staticmethod
    @abstractmethod
    def name() -> str:
        """
        Return the name of the embedding function.
        """
        ...

    def default_space(self) -> Space:
        """
        Return the default space for the embedding function.
        """
        return Space.COSINE

    def supported_spaces(self) -> List[Space]:
        """
        Return the supported spaces for the embedding function.
        """
        return [Space.COSINE, Space.L2, Space.INNER_PRODUCT]

    @staticmethod
    @abstractmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[D]":
        """
        Build the embedding function from a config, which will be used to
        deserialize the embedding function.
        """
        ...

    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        """
        Return the config for the embedding function, which will be used to
        serialize the embedding function.
        """
        ...

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        """
        Validate the update to the config.
        """
        return

    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate the config.
        """
        return
