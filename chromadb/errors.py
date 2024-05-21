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
    def name(cls) -> str:
        """Return the error name"""
        pass


class InvalidDimensionException(ChromaError):
    @classmethod
    @overrides
    def name(cls) -> str:
        return "InvalidDimension"


class InvalidCollectionException(ChromaError):
    @classmethod
    @overrides
    def name(cls) -> str:
        return "InvalidCollection"


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


class InvalidHTTPVersion(ChromaError):
    @classmethod
    @overrides
    def name(cls) -> str:
        return "InvalidHTTPVersion"


class AuthorizationError(ChromaError):
    @overrides
    def code(self) -> int:
        return 401

    @classmethod
    @overrides
    def name(cls) -> str:
        return "AuthorizationError"


error_types: Dict[str, Type[ChromaError]] = {
    "InvalidDimension": InvalidDimensionException,
    "InvalidCollection": InvalidCollectionException,
    "IDAlreadyExists": IDAlreadyExistsError,
    "DuplicateID": DuplicateIDError,
    "InvalidUUID": InvalidUUIDError,
    "InvalidHTTPVersion": InvalidHTTPVersion,
    "AuthorizationError": AuthorizationError,
}
