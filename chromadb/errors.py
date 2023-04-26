from abc import ABCMeta, abstractmethod


class ChromaError(Exception):

    def code(self):
        """Return an appropriate HTTP response code for this error"""
        return 400 # Bad Request

    def message(self):
        return ", ".join(self.args)

    @classmethod
    @abstractmethod
    def name(self):
        """Return the error name"""
        pass


class NoDatapointsException(ChromaError):
    @classmethod
    def name(cls):
        return "NoDatapoints"


class NoIndexException(ChromaError):
    @classmethod
    def name(cls):
        return "NoIndex"


class InvalidDimensionException(ChromaError):
    @classmethod
    def name(cls):
        return "InvalidDimension"


class NotEnoughElementsException(ChromaError):
    @classmethod
    def name(cls):
        return "NotEnoughElements"


class IDAlreadyExistsError(ChromaError):

    def code(self):
        return 409 # Conflict

    @classmethod
    def name(cls):
        return "IDAlreadyExists"


class DuplicateIDError(ChromaError):
    @classmethod
    def name(cls):
        return "DuplicateID"

error_types = {
    "NoDatapoints": NoDatapointsException,
    "NoIndex": NoIndexException,
    "InvalidDimension": InvalidDimensionException,
    "NotEnoughElements": NotEnoughElementsException,
    "IDAlreadyExists": IDAlreadyExistsError,
    "DuplicateID": DuplicateIDError,
}
