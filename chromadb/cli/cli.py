import time

import requests
import typer
import uvicorn
import os
import webbrowser

import chromadb
from chromadb.utils import system_info_utils

try:
    from rich import print as rprint
except ImportError:
    rprint = typer.echo


def format_size(size: int) -> str:
    try:
        from humanfriendly import format_size as hf_format_size

        return str(hf_format_size(size))
    except ImportError:
        return str(size)


app = typer.Typer()

_logo = """
                \033[38;5;069m(((((((((    \033[38;5;203m(((((\033[38;5;220m####
             \033[38;5;069m(((((((((((((\033[38;5;203m(((((((((\033[38;5;220m#########
           \033[38;5;069m(((((((((((((\033[38;5;203m(((((((((((\033[38;5;220m###########
         \033[38;5;069m((((((((((((((\033[38;5;203m((((((((((((\033[38;5;220m############
        \033[38;5;069m(((((((((((((\033[38;5;203m((((((((((((((\033[38;5;220m#############
        \033[38;5;069m(((((((((((((\033[38;5;203m((((((((((((((\033[38;5;220m#############
         \033[38;5;069m((((((((((((\033[38;5;203m(((((((((((((\033[38;5;220m##############
         \033[38;5;069m((((((((((((\033[38;5;203m((((((((((((\033[38;5;220m##############
           \033[38;5;069m((((((((((\033[38;5;203m(((((((((((\033[38;5;220m#############
             \033[38;5;069m((((((((\033[38;5;203m((((((((\033[38;5;220m##############
                \033[38;5;069m(((((\033[38;5;203m((((    \033[38;5;220m#########\033[0m

    """


@app.command()  # type: ignore
def run(
    path: str = typer.Option(
        "./chroma_data", help="The path to the file or directory."
    ),
    port: int = typer.Option(8000, help="The port to run the server on."),
    test: bool = typer.Option(False, help="Test mode.", show_envvar=False, hidden=True),
) -> None:
    """Run a chroma server"""

    print("\033[1m")  # Bold logo
    print(_logo)
    print("\033[1m")  # Bold
    print("Running Chroma")
    print("\033[0m")  # Reset

    typer.echo(f"\033[1mSaving data to\033[0m: \033[32m{path}\033[0m")
    typer.echo(
        f"\033[1mConnect to chroma at\033[0m: \033[32mhttp://localhost:{port}\033[0m"
    )
    typer.echo(
        "\033[1mGetting started guide\033[0m: https://docs.trychroma.com/getting-started\n\n"
    )

    # set ENV variable for PERSIST_DIRECTORY to path
    os.environ["IS_PERSISTENT"] = "True"
    os.environ["PERSIST_DIRECTORY"] = path

    # get the path where chromadb is installed
    chromadb_path = os.path.dirname(os.path.realpath(__file__))

    # this is the path of the CLI, we want to move up one directory
    chromadb_path = os.path.dirname(chromadb_path)

    config = {
        "app": "chromadb.app:app",
        "host": "0.0.0.0",
        "port": port,
        "workers": 1,
        "log_config": f"{chromadb_path}/log_config.yml",
    }

    if test:
        return

    uvicorn.run(**config)


@app.command(help="Local and remote Chroma system information")  # type: ignore
def system_info(
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
            f"{remote}/api/v1/system-info?python_version={python_version}&os_info={os_info}&memory_info={memory_info}"
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
            client.get_system_info(
                python_version=python_version,
                os_info=os_info,
                memory_info=memory_info,
                cpu_info=cpu_info,
                disk_info=disk_info,
                network_info=network_info,
                env_vars=env_vars,
                collections_info=collections_info,
            )
        )
    else:
        rprint(
            system_info_utils.system_info(
                python_version=python_version,
                os_info=os_info,
                memory_info=memory_info,
                cpu_info=cpu_info,
                disk_info=disk_info,
                network_info=network_info,
                env_vars=env_vars,
                collections_info=collections_info,
                api=None,
            )
        )
    typer.echo(
        "===================================== End system info ====================================="
    )


@app.command(help="Remote chroma continuous monitoring. Prints out CPU and memory usage")  # type: ignore
def rstat(
    remote: str = typer.Option(
        ..., help="Remote Chroma server to connect to.", show_envvar=False, hidden=True
    ),
    interval: int = typer.Option(1, help="Interval in seconds."),
) -> None:
    while True:
        remote_response = requests.get(
            f"{remote}/api/v1/system-info?python_version=false&os_info=false&memory_info=true"
            f"&cpu_info=true&disk_info=false&network_info=false&env_vars=false"
            f"&collections_info=false"
        )
        if remote_response.status_code != 200:
            typer.echo(f"Error: {remote_response.text}")
            raise typer.Exit(code=1)
        _json = remote_response.json()
        rprint(
            f'{_json["cpu_info"]["cpu_usage"]} % \t'
            f'{format_size(remote_response.json()["memory_info"]["process_memory"]["rss"])}'
        )
        time.sleep(interval)


@app.command()  # type: ignore
def help() -> None:
    """Opens help url in your browser"""

    webbrowser.open("https://discord.gg/MMeYNTmh3x")


@app.command()  # type: ignore
def docs() -> None:
    """Opens docs url in your browser"""

    webbrowser.open("https://docs.trychroma.com")


if __name__ == "__main__":
    app()
