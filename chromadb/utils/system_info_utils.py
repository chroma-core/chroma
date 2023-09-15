import datetime
import logging
import os
import platform
import socket
from typing import Dict, Any, Optional
import re
import chromadb
from chromadb.api import API

logger = logging.getLogger(__name__)

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


def system_info(
    api: Optional[API] = None,
    python_version: bool = True,
    os_info: bool = True,
    memory_info: bool = True,
    cpu_info: bool = True,
    disk_info: bool = True,
    network_info: bool = True,
    env_vars: bool = True,
    collections_info: bool = True,
) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "chroma_version": chromadb.__version__,
        "datetime": datetime.datetime.now().isoformat(),
    }

    if os.environ.get("PERSIST_DIRECTORY") or api:
        data["persist_directory"] = os.environ.get("PERSIST_DIRECTORY") or (
            api.get_settings().persist_directory if api else ""
        )

    if python_version:
        data["python_version"] = platform.python_version()

    if os_info:
        data["os"] = platform.system()
        data["os_version"] = platform.release()
        data["os_release"] = get_release_info(system=platform.system())

    if memory_info and PSUTIL_INSTALLED:
        mem = psutil.virtual_memory()
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        data["memory_info"] = {
            "free_memory": mem.available,
            "total_memory": mem.total,
        }
        data["memory_info"]["process_memory"] = {
            "rss": memory_info.rss,  # type: ignore
            "vms": memory_info.vms,  # type: ignore
        }

    if cpu_info:
        data["cpu_info"] = {
            "architecture": platform.machine(),
            "number_of_cpus": os.cpu_count(),
        }
        if PSUTIL_INSTALLED:
            data["cpu_info"]["cpu_usage"] = psutil.cpu_percent(interval=1)

    if disk_info and PSUTIL_INSTALLED:
        disk = psutil.disk_usage("/")
        data["disk_info"] = {
            "total_space": disk.total,
            "used_space": disk.used,
            "free_space": disk.free,
        }

    if network_info and PSUTIL_INSTALLED:
        ip_info = {
            interface: [addr.address for addr in addrs if addr.family == socket.AF_INET]
            for interface, addrs in psutil.net_if_addrs().items()
        }
        data["network_info"] = ip_info

    if env_vars:
        data["env_vars"] = sanitized_environ()

    if collections_info and api:
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
