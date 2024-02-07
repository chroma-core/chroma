import datetime
import logging
import os
import platform
import psutil
from typing import Dict, cast, Any
import chromadb
from chromadb.api.types import SystemInfo, OperatingMode
from chromadb.api import ServerAPI as API

logger = logging.getLogger(__name__)


def format_size(size_in_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(size_in_bytes)

    while size > 1024 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1

    return f"{size:.2f} {units[unit_index]}"


def get_release_info(system: str) -> str:
    release = f"Unknown OS Release {platform.release()}"
    if system == "Linux":
        try:
            with open("/etc/os-release") as f:
                for line in f:
                    if line.startswith("PRETTY_NAME"):
                        release = line.split("=")[1].strip().strip('"')
                        break
        except Exception:
            pass
    elif system == "Darwin":
        release = (
            os.popen("sw_vers")
            .read()
            .strip()
            .replace("\t\t", " ")
            .replace("\t", " ")
            .replace("\n", " ")
        )
    elif system == "Windows":
        release = platform.release()
    return release


def system_info(api: API) -> SystemInfo:
    data: Dict[str, Any] = dict()
    data["chroma_version"] = chromadb.__version__
    data["python_version"] = platform.python_version()
    data["is_persistent"] = api.get_settings().is_persistent
    data["api"] = api.get_settings().chroma_api_impl
    data["datetime"] = datetime.datetime.now().isoformat()
    data["os"] = platform.system()
    data["os_version"] = platform.release()
    data["os_release"] = get_release_info(system=platform.system())
    mem = psutil.virtual_memory()
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    data["memory_free"] = mem.available
    data["memory_total"] = mem.total
    data["process_memory_rss"] = memory_info.rss
    data["process_memory_vms"] = memory_info.vms
    data["cpu_architecture"] = platform.machine()
    data["cpu_count"] = os.cpu_count()
    data["cpu_usage"] = psutil.cpu_percent(interval=1)
    disk_info = None
    if (
        api
        and api.get_settings().is_persistent
        and api.get_settings().persist_directory
        and os.path.exists(api.get_settings().persist_directory)
    ):
        disk_info = psutil.disk_usage(api.get_settings().persist_directory)

    data["persistent_disk_free"] = disk_info.free if disk_info else None
    data["persistent_disk_total"] = disk_info.total if disk_info else None
    data["persistent_disk_used"] = disk_info.used if disk_info else None

    # local mode either to the server or the client, TBD for distributed - perhaps we can check on api impl
    if (
        api.get_settings().chroma_server_backend_impl
        == "chromadb.server.fastapi.FastAPI"
        and api.get_settings().chroma_segment_manager_impl
        == "chromadb.segment.impl.manager.local.LocalSegmentManager"
    ):
        data["mode"] = OperatingMode.SINGLE_NODE_SERVER
    elif (
        api.get_settings().chroma_server_backend_impl
        == "chromadb.server.fastapi.FastAPI"
        and api.get_settings().chroma_segment_manager_impl
        == "chromadb.segment.impl.manager.distributed.DistributedSegmentManager"
    ):
        data["mode"] = OperatingMode.DISTRIBUTED_SERVER
    elif api.get_settings().chroma_api_impl == "chromadb.api.fastapi.FastAPI":
        data["mode"] = OperatingMode.HTTP_CLIENT
    elif api.get_settings().is_persistent:
        data["mode"] = OperatingMode.PERSISTENT_CLIENT
    else:
        data["mode"] = OperatingMode.EPHEMERAL_CLIENT

    return cast(SystemInfo, data)
