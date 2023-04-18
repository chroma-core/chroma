class NoDatapointsException(Exception):
    pass


class NoIndexException(Exception):
    pass


class InvalidDimensionException(Exception):
    pass


class NotEnoughElementsException(Exception):
    pass


class IDAlreadyExistsError(ValueError):
    pass


class DuplicateIDError(ValueError):
    pass
