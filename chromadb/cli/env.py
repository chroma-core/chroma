import json
import os
import traceback

import typer
import chromadb

env_app = typer.Typer()

try:
    from rich import print as rprint
except ImportError:
    rprint = typer.echo


def info(
    remote: str = typer.Option(
        None,
        help="Remote Chroma server to connect to.",
    ),
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
        _env = client.env()
        if "server" in _env.keys() and _env["server"] is not None:
            typer.echo(
                "================================== Remote Sever system info ======================================"
            )
            rprint(json.dumps(_env["server"], indent=4))
        if "client" in _env.keys():
            typer.echo(
                "================================== Local client system info ======================================"
            )
            rprint(json.dumps(_env["client"], indent=4))
        typer.echo(
            "=================================================================================================="
        )
    except Exception as e:
        traceback.print_exc()
        typer.echo(f"Failed to get system info {type(client)}: {str(e)}")
