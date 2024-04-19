from abc import abstractmethod
from typing import Any, Dict, Generic, Protocol, TypeVar, cast
import json

T = TypeVar("T", bound="JSONSerializable", covariant=True)


class JSONSerializable(Protocol[T]):
    """A generic interface for objects that can be serialized to JSON"""

    def to_json_str(self) -> str:
        """Serializes the object to JSON"""
        ...

    def to_json(self) -> Dict[str, Any]:
        """Serializes the object to a JSON compatible dictionary"""
        ...

    @classmethod
    def from_json(cls, json_map: Dict[str, Any]) -> T:
        """Deserializes the object from JSON"""
        ...


class BaseModelJSONSerializable(Generic[T]):
    """A mixin for BaseModels that allows a class to be serialized to JSON"""

    def to_json_str(self) -> str:
        """Serializes the object to JSON"""
        return self.model_dump_json()

    def to_json(self) -> Dict[str, Any]:
        """Serializes the object to a JSON compatible dictionary"""
        return json.loads(self.model_dump_json())

    @abstractmethod
    def model_dump_json(self) -> str:
        """Abstract method that should be implemented to dump the model to JSON"""
        pass

    @classmethod
    def from_json(cls, json_map: Dict[str, Any]) -> T:
        """Deserializes the object from JSON"""
        return cast(T, cls(**json_map))
