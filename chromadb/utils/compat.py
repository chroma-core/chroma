"""Compatibility layer for ``@override`` / ``EnforceOverrides``.

On Python 3.12+ we use the standard ``typing.override`` decorator so that
Chroma works on Python 3.14 where the third-party ``overrides`` package
currently crashes (see python/cpython#118803).

On Python < 3.12 we fall back to the ``overrides`` package so that the
behaviour is unchanged for existing supported runtimes.
"""

import sys

if sys.version_info >= (3, 12):
    from typing import override

    class EnforceOverrides:
        """No-op replacement for ``overrides.EnforceOverrides``."""

        pass

    # The ``overrides`` package exposes both spellings; keep them available.
    overrides = override

else:
    from overrides import override, EnforceOverrides, overrides  # type: ignore[no-redef,assignment]
