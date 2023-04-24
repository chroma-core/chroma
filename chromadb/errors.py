class NoDatapointsException(Exception):
    pass


class NoIndexException(Exception):
    pass


class InvalidDimensionException(Exception):
    pass


class NotEnoughElementsException(Exception):
    pass


class IDAlreadyExistsError(ValueError):
    """ID already exists in the collection."""

    pass


class DuplicateIDError(ValueError):
    """Duplicate IDs in an operation."""

    pass
