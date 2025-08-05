"""
build - A simple, correct Python build frontend
"""

from __future__ import annotations

from ._builder import ProjectBuilder
from ._exceptions import (
    BuildBackendException,
    BuildException,
    BuildSystemTableValidationError,
    FailedProcessError,
    TypoWarning,
)
from ._types import ConfigSettings as ConfigSettingsType
from ._types import Distribution as DistributionType
from ._types import SubprocessRunner as RunnerType
from ._util import check_dependency


__version__ = '1.3.0'

__all__ = [
    'BuildBackendException',
    'BuildException',
    'BuildSystemTableValidationError',
    'ConfigSettingsType',
    'DistributionType',
    'FailedProcessError',
    'ProjectBuilder',
    'RunnerType',
    'TypoWarning',
    '__version__',
    'check_dependency',
]


def __dir__() -> list[str]:
    return __all__
