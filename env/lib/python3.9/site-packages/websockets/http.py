from __future__ import annotations

import warnings

from .datastructures import Headers, MultipleValuesError  # noqa: F401
from .legacy.http import read_request, read_response  # noqa: F401


warnings.warn(
    "Headers and MultipleValuesError were moved "
    "from websockets.http to websockets.datastructures"
    "and read_request and read_response were moved "
    "from websockets.http to websockets.legacy.http",
    DeprecationWarning,
)
