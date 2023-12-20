import json
import logging
from typing import List
from urllib import request

logger = logging.getLogger(__name__)


def compare_versions(version1: str, version2: str) -> int:
    """Compares two versions of the format X.Y.Z and returns 1 if version1 is greater than version2, -1 if version1 is
    less than version2, and 0 if version1 is equal to version2.
    """
    v1_components = list(map(int, version1.split(".")))
    v2_components = list(map(int, version2.split(".")))

    for v1, v2 in zip(v1_components, v2_components):
        if v1 > v2:
            return 1
        elif v1 < v2:
            return -1

    if len(v1_components) > len(v2_components):
        return 1
    elif len(v1_components) < len(v2_components):
        return -1

    return 0


_upgrade_check_url: str = "https://pypi.org/pypi/chromadb/json"
_check_performed: bool = False


def _upgrade_check() -> List[str]:
    """Check pypi index for new version if possible."""
    global _check_performed
    upgrade_messages: List[str] = []
    # this is to prevent cli from double printing
    if _check_performed:
        return upgrade_messages
    try:
        data = json.load(
            request.urlopen(request.Request(_upgrade_check_url), timeout=5)
        )
        from chromadb import __version__ as local_chroma_version

        latest_version = data["info"]["version"]
        if compare_versions(latest_version, local_chroma_version) > 0:
            upgrade_messages.append(
                f"\033[38;5;069m[notice]\033[0m A new release of chromadb is available: "
                f"\033[38;5;196m{local_chroma_version}\033[0m -> "
                f"\033[38;5;082m{latest_version}\033[0m"
            )
            upgrade_messages.append(
                "\033[38;5;069m[notice]\033[0m To upgrade, run `pip install --upgrade chromadb`."
            )
    except Exception:
        pass
    _check_performed = True
    for m in upgrade_messages:
        logger.info(m)
    return upgrade_messages
