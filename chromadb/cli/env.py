import json
import os
import time
import typer
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
        None,
        help="Remote Chroma server to connect to.",
    ),
    python_version: bool = typer.Option(True, help="Show python version."),
    os_info: bool = typer.Option(True, help="Show os info."),
    memory_info: bool = typer.Option(True, help="Show memory info."),
    cpu_info: bool = typer.Option(True, help="Show cpu info."),
    disk_info: bool = typer.Option(True, help="Show disk info."),
    path: str = typer.Option(None, help="The path to local persistence directory."),
) -> None:
    if remote:
        client = chromadb.HttpClient(
            host=remote, port=f"{os.environ.get('CHROMA_SERVER_HTTP_PORT', 8000)}"
        )
    elif path:
        typer.echo(f"Local persistent client with path: {path}")
        if not os.path.exists(path):
            typer.echo(f"Error: {path} does not exist")
            raise typer.Exit(code=1)
        client = chromadb.PersistentClient(path=path)
    else:
        client = chromadb.Client()
    try:
        _env = client.env(
            system_info_flags=SystemInfoFlags(
                python_version=python_version,
                os_info=os_info,
                memory_info=memory_info,
                cpu_info=cpu_info,
                disk_info=disk_info,
            ),
        )
        if "server" in _env.keys():
            typer.echo(
                "================================== Remote Sever system info =================================="
            )
            rprint(json.dumps(_env["server"], indent=4))
            typer.echo(
                "================================== End Remote Sever system info =================================="
            )
        if "client" in _env.keys():
            typer.echo(
                "================================== Local client system info =================================="
            )
            rprint(json.dumps(_env["client"], indent=4))
            typer.echo(
                "================================== End local system info =================================="
            )
    except Exception as e:
        typer.echo(f"Failed to get system info: {str(e)}")


@env_app.command(help="Remote chroma continuous monitoring. Prints out CPU and memory usage")  # type: ignore
def rstat(
    remote: str = typer.Option(..., help="Remote Chroma server to connect to."),
    interval: int = typer.Option(1, help="Interval in seconds."),
) -> None:
    system_info_flags = SystemInfoFlags(
        python_version=False,
        os_info=True,
        memory_info=True,
        cpu_info=True,
        disk_info=True,
    )
    client = chromadb.HttpClient(host=remote)

    while True:
        try:
            remote_response = client.env(system_info_flags=system_info_flags)
            if "server" in remote_response.keys():
                rprint(
                    f'{remote_response["server"]["cpu_info"]["cpu_usage"]} % \t'
                    f'{system_info_utils.format_size(remote_response["server"]["memory_info"]["process_memory"]["rss"])}'
                )
            time.sleep(interval)
        except Exception as e:
            typer.echo(f"Failed to get system info: {e}")
            raise typer.Exit(code=1)
