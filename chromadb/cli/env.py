import os
import time
import typer
import requests
import chromadb
from chromadb.api.types import SystemInfoFlags
from chromadb.utils import system_info_utils

env_app = typer.Typer()

try:
    from rich import print as rprint
except ImportError:
    rprint = typer.echo


@env_app.command(help="Local and remote Chroma system information")  # type: ignore
def info(
    remote: str = typer.Option(
        None, help="Remote Chroma server to connect to.", show_envvar=False, hidden=True
    ),
    python_version: bool = typer.Option(True, help="Show python version."),
    os_info: bool = typer.Option(True, help="Show os info."),
    memory_info: bool = typer.Option(True, help="Show memory info."),
    cpu_info: bool = typer.Option(True, help="Show cpu info."),
    disk_info: bool = typer.Option(False, help="Show disk info."),
    network_info: bool = typer.Option(False, help="Show network info."),
    env_vars: bool = typer.Option(False, help="Show env vars."),
    collections_info: bool = typer.Option(
        False, help="Show collections info. Works only for remote Chroma servers."
    ),
    path: str = typer.Option(None, help="The path to local persistence directory."),
) -> None:
    if remote:
        remote_response = requests.get(
            f"{remote}/api/v1/env?python_version={python_version}&os_info={os_info}&memory_info={memory_info}"
            f"&cpu_info={cpu_info}&disk_info={disk_info}&network_info={network_info}&env_vars={env_vars}"
            f"&collections_info={collections_info}"
        )
        if remote_response.status_code != 200:
            typer.echo(f"Error: {remote_response.text}")
            raise typer.Exit(code=1)
        typer.echo(
            "===================================== Remote system info ====================================="
        )
        rprint(remote_response.json())
    typer.echo(
        "===================================== Local system info ====================================="
    )
    if path:
        typer.echo(f"Local persistent client with path: {path}")
        if not os.path.exists(path):
            typer.echo(f"Error: {path} does not exist")
            raise typer.Exit(code=1)
        client = chromadb.PersistentClient(path=path)
        rprint(
            client.env(
                system_info_flags=SystemInfoFlags(
                    python_version=python_version,
                    os_info=os_info,
                    memory_info=memory_info,
                    cpu_info=cpu_info,
                    disk_info=disk_info,
                    network_info=network_info,
                    env_vars=env_vars,
                    collections_info=collections_info,
                ),
            )
        )
    else:
        rprint(
            system_info_utils.system_info(
                system_info_flags=SystemInfoFlags(
                    python_version=python_version,
                    os_info=os_info,
                    memory_info=memory_info,
                    cpu_info=cpu_info,
                    disk_info=disk_info,
                    network_info=network_info,
                    env_vars=env_vars,
                    collections_info=collections_info,
                ),
                api=None,
            )
        )
    typer.echo(
        "===================================== End system info ====================================="
    )


@env_app.command(help="Remote chroma continuous monitoring. Prints out CPU and memory usage")  # type: ignore
def rstat(
    remote: str = typer.Option(
        ..., help="Remote Chroma server to connect to.", show_envvar=False, hidden=True
    ),
    interval: int = typer.Option(1, help="Interval in seconds."),
) -> None:
    system_info_flags = SystemInfoFlags(
        python_version=False,
        os_info=False,
        memory_info=True,
        cpu_info=True,
        disk_info=False,
        network_info=False,
        env_vars=False,
        collections_info=False,
    )
    params = {
        field: getattr(system_info_flags, field) for field in system_info_flags._fields
    }
    while True:
        remote_response = requests.get(f"{remote}/api/v1/env", params=params)
        if remote_response.status_code != 200:
            typer.echo(f"Error: {remote_response.text}")
            raise typer.Exit(code=1)
        _json = remote_response.json()
        rprint(
            f'{_json["cpu_info"]["cpu_usage"]} % \t'
            f'{system_info_utils.format_size(remote_response.json()["memory_info"]["process_memory"]["rss"])}'
        )
        time.sleep(interval)
