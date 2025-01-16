from logging import getLogger
from typing import Collection, Optional

from pkg_resources import (
    Distribution,
    DistributionNotFound,
    RequirementParseError,
    VersionConflict,
    get_distribution,
)

logger = getLogger(__name__)


class DependencyConflict:
    required: str = None
    found: Optional[str] = None

    def __init__(self, required, found=None):
        self.required = required
        self.found = found

    def __str__(self):
        return f'DependencyConflict: requested: "{self.required}" but found: "{self.found}"'


def get_dist_dependency_conflicts(
    dist: Distribution,
) -> Optional[DependencyConflict]:
    main_deps = dist.requires()
    instrumentation_deps = []
    for dep in dist.requires(("instruments",)):
        if dep not in main_deps:
            # we set marker to none so string representation of the dependency looks like
            #    requests ~= 1.0
            # instead of
            #    requests ~= 1.0; extra = "instruments"
            # which does not work with `get_distribution()`
            dep.marker = None
            instrumentation_deps.append(str(dep))

    return get_dependency_conflicts(instrumentation_deps)


def get_dependency_conflicts(
    deps: Collection[str],
) -> Optional[DependencyConflict]:
    for dep in deps:
        try:
            get_distribution(dep)
        except VersionConflict as exc:
            return DependencyConflict(dep, exc.dist)
        except DistributionNotFound:
            return DependencyConflict(dep)
        except RequirementParseError as exc:
            logger.warning(
                'error parsing dependency, reporting as a conflict: "%s" - %s',
                dep,
                exc,
            )
            return DependencyConflict(dep)
    return None
