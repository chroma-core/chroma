from abc import abstractmethod
from typing import Dict, Type
from overrides import overrides, EnforceOverrides


class ChromaError(Exception, EnforceOverrides):
    def code(self) -> int:
        """Return an appropriate HTTP response code for this error"""
        return 400  # Bad Request

    def message(self) -> str:
        return ", ".join(self.args)

    @classmethod
    @abstractmethod
    def name(self) -> str:
        """Return the error name"""
        pass


class InvalidDimensionException(ChromaError):
    @classmethod
    @overrides
    def name(cls) -> str:
        return "InvalidDimension"


class IDAlreadyExistsError(ChromaError):
    @overrides
    def code(self) -> int:
        return 409  # Conflict

    @classmethod
    @overrides
    def name(cls) -> str:
        return "IDAlreadyExists"


class DuplicateIDError(ChromaError):
    @classmethod
    @overrides
    def name(cls) -> str:
        return "DuplicateID"


class InvalidUUIDError(ChromaError):
    @classmethod
    @overrides
    def name(cls) -> str:
        return "InvalidUUID"


error_types: Dict[str, Type[ChromaError]] = {
    "InvalidDimension": InvalidDimensionException,
    "IDAlreadyExists": IDAlreadyExistsError,
    "DuplicateID": DuplicateIDError,
    "InvalidUUID": InvalidUUIDError,
}
