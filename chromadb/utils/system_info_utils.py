import datetime
import logging
import os
import platform
import socket
from typing import Dict, Any, Optional, cast
import re
import chromadb
from chromadb.api.types import SystemInfoFlags
from chromadb.config import Settings
from chromadb.api import API

logger = logging.getLogger(__name__)


def format_size(size_in_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(size_in_bytes)

    while size > 1024 and unit_index < 4:
        size /= 1024.0
        unit_index += 1

    return f"{size:.2f} {units[unit_index]}"


try:
    import psutil

    PSUTIL_INSTALLED = True
except ImportError:
    PSUTIL_INSTALLED = False
    logger.warning(
        "psutil is not installed. Some system info won't be available. To install psutil, run "
        "'pip install psutil'."
    )

SENSITIVE_VARS_PATTERNS = [".*PASSWORD.*", ".*KEY.*", ".*AUTH.*"]
SENSITIVE_SETTINGS_PATTERNS = [".*credentials.*"]


def sanitized_environ() -> Dict[str, str]:
    env = dict(os.environ)
    for key in env.keys():
        if any(re.match(pattern, key) for pattern in SENSITIVE_VARS_PATTERNS):
            env[key] = "*****"
    return env


def get_release_info(system: str) -> str:
    if system == "Linux":
        with open("/etc/os-release") as f:
            for line in f:
                if line.startswith("PRETTY_NAME"):
                    return line.split("=")[1].strip().strip('"')
        return "Unknown Linux Distro"
    elif system == "Darwin":
        return (
            os.popen("sw_vers")
            .read()
            .strip()
            .replace("\t\t", " ")
            .replace("\t", " ")
            .replace("\n", " ")
        )
    elif system == "Windows":
        return platform.release()
    else:
        return "Unknown OS"


def sanitize_settings(settings: Settings) -> Dict[str, Any]:
    _settings_dict = settings.dict()
    for key in _settings_dict.keys():
        if any(re.match(pattern, key) for pattern in SENSITIVE_VARS_PATTERNS):
            _settings_dict[key] = "*****"
        if any(re.match(pattern, key) for pattern in SENSITIVE_SETTINGS_PATTERNS):
            _settings_dict[key] = "*****"
    return cast(Dict[str, Any], _settings_dict)


def system_info(
    api: Optional[API] = None,
    system_info_flags: Optional[SystemInfoFlags] = None,
) -> Dict[str, Any]:
    _flags = system_info_flags or SystemInfoFlags()
    data: Dict[str, Any] = {
        "chroma_version": chromadb.__version__,
        "chroma_settings": sanitize_settings(api.get_settings()) if api else {},
        "datetime": datetime.datetime.now().isoformat(),
    }

    if os.environ.get("PERSIST_DIRECTORY") or api:
        data["persist_directory"] = os.environ.get("PERSIST_DIRECTORY") or (
            api.get_settings().persist_directory if api else ""
        )

    if _flags.python_version:
        data["python_version"] = platform.python_version()

    if _flags.os_info:
        data["os"] = platform.system()
        data["os_version"] = platform.release()
        data["os_release"] = get_release_info(system=platform.system())

    if _flags.memory_info and PSUTIL_INSTALLED:
        mem = psutil.virtual_memory()
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        data["memory_info"] = {
            "free_memory": mem.available,
            "total_memory": mem.total,
        }
        data["memory_info"]["process_memory"] = {
            "rss": memory_info.rss,
            "vms": memory_info.vms,
        }

    if _flags.cpu_info:
        data["cpu_info"] = {
            "architecture": platform.machine(),
            "number_of_cpus": os.cpu_count(),
        }
        if PSUTIL_INSTALLED:
            data["cpu_info"]["cpu_usage"] = psutil.cpu_percent(interval=1)

    if _flags.disk_info and PSUTIL_INSTALLED:
        disk = psutil.disk_usage("/")
        data["disk_info"] = {
            "total_space": disk.total,
            "used_space": disk.used,
            "free_space": disk.free,
        }

    if _flags.network_info and PSUTIL_INSTALLED:
        ip_info = {
            interface: [addr.address for addr in addrs if addr.family == socket.AF_INET]
            for interface, addrs in psutil.net_if_addrs().items()
        }
        data["network_info"] = ip_info

    if _flags.env_vars:
        data["env_vars"] = sanitized_environ()

    if _flags.collections_info and api:
        data["collections_info"] = [
            {
                "name": collection.name,
                "id": collection.id,
                "count": api._count(collection.id),
                "metadata": collection.metadata,
            }
            for collection in api.list_collections()
        ]
    return data
