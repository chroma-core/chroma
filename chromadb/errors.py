from abc import abstractmethod


class ChromaError(Exception):
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


class NoDatapointsException(ChromaError):
    @classmethod
    def name(cls) -> str:
        return "NoDatapoints"


class NoIndexException(ChromaError):
    @classmethod
    def name(cls) -> str:
        return "NoIndex"


class InvalidDimensionException(ChromaError):
    @classmethod
    def name(cls) -> str:
        return "InvalidDimension"


class NotEnoughElementsException(ChromaError):
    @classmethod
    def name(cls) -> str:
        return "NotEnoughElements"


class IDAlreadyExistsError(ChromaError):
    def code(self) -> int:
        return 409  # Conflict

    @classmethod
    def name(cls) -> str:
        return "IDAlreadyExists"


class DuplicateIDError(ChromaError):
    @classmethod
    def name(cls) -> str:
        return "DuplicateID"


class InvalidUUIDError(ChromaError):
    @classmethod
    def name(cls) -> str:
        return "InvalidUUID"


error_types = {
    "NoDatapoints": NoDatapointsException,
    "NoIndex": NoIndexException,
    "InvalidDimension": InvalidDimensionException,
    "NotEnoughElements": NotEnoughElementsException,
    "IDAlreadyExists": IDAlreadyExistsError,
    "DuplicateID": DuplicateIDError,
    "InvalidUUID": InvalidUUIDError,
}
