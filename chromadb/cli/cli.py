from typing import Optional

import chromadb_rust_bindings
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
import typer.rich_utils
from typing_extensions import Annotated
import typer
import uvicorn
import os
import webbrowser

from chromadb.api.client import Client
from chromadb.cli.utils import get_directory_size, set_log_file_path, sizeof_fmt
from chromadb.config import Settings, System
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.ingest.impl.utils import trigger_vector_segments_max_seq_id_migration
from chromadb.segment import SegmentManager

app = typer.Typer()
utils_app = typer.Typer(short_help="Use maintenance utilities")
app.add_typer(utils_app, name="utils")


def build_cli_args(**kwargs):
    args = []
    for key, value in kwargs.items():
        if isinstance(value, bool):
            if value:
                args.append(f"--{key}")
        elif value is not None:
            args.extend([f"--{key}", str(value)])
    return args


@app.command()  # type: ignore
def run(
    path: str = typer.Option(
        "./chroma_data", help="The path to the file or directory."
    ),
    host: Annotated[
        Optional[str], typer.Option(help="The host to listen to. Default: localhost")
    ] = "localhost",
    port: int = typer.Option(8000, help="The port to run the server on."),
    test: bool = typer.Option(False, help="Test mode.", show_envvar=False, hidden=True),
) -> None:
    """Run a chroma server"""
    cli_args = ["chroma", "run"]
    cli_args.extend(build_cli_args(
        path=path,
        host=host,
        port=port,
        test=test
    ))
    chromadb_rust_bindings.run_cli(cli_args)


@utils_app.command()  # type: ignore
def vacuum(
    path: str = typer.Option(
        help="The path to a Chroma data directory.",
    ),
    force: bool = typer.Option(False, help="Force vacuuming without confirmation."),
) -> None:
    """
    Vacuum the database. This may result in a small increase in performance.

    If you recently upgraded Chroma from a version below 0.5.6 to 0.5.6 or above, you should run this command once to greatly reduce the size of your database and enable continuous database pruning. In most other cases, vacuuming will save very little disk space.

    The execution time of this command scales with the size of your database. It blocks both reads and writes to the database while it is running.
    """
    console = Console(
        highlight=False
    )  # by default, rich highlights numbers which makes the output look weird when we try to color numbers ourselves

    if not os.path.exists(path):
        console.print(f"[bold red]Path {path} does not exist.[/bold red]")
        raise typer.Exit(code=1)

    if not os.path.exists(f"{path}/chroma.sqlite3"):
        console.print(
            f"[bold red]Path {path} is not a Chroma data directory.[/bold red]"
        )
        raise typer.Exit(code=1)

    if not force and not typer.confirm(
        "Are you sure you want to vacuum the database? This will block both reads and writes to the database and may take a while. We recommend shutting down the server before running this command. Continue?",
    ):
        console.print("Vacuum cancelled.")
        raise typer.Exit(code=0)

    settings = Settings()
    settings.is_persistent = True
    settings.persist_directory = path
    system = System(settings=settings)
    system.start()
    client = Client.from_system(system)
    sqlite = system.instance(SqliteDB)

    directory_size_before_vacuum = get_directory_size(path)

    console.print()  # Add a newline before the progress bar

    with Progress(
        SpinnerColumn(finished_text="[bold green]:heavy_check_mark:[/bold green]"),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        collections = client.list_collections()
        task = progress.add_task("Purging the log...", total=len(collections))
        try:
            # Cleaning the log after upgrading to >=0.5.6 is dependent on vector segments migrating their max_seq_id from the pickled metadata file to SQLite.
            # Vector segments migrate this field automatically on init, but at this point the segment has not been loaded yet.
            trigger_vector_segments_max_seq_id_migration(
                sqlite, system.instance(SegmentManager)
            )

            for collection_name in collections:
                collection = client.get_collection(collection_name)
                sqlite.purge_log(collection_id=collection.id)
                progress.update(task, advance=1)
        except Exception as e:
            console.print(f"[bold red]Error purging the log:[/bold red] {e}")
            raise typer.Exit(code=1)

        task = progress.add_task("Vacuuming (this may take a while)...")
        try:
            sqlite.vacuum()
            config = sqlite.config
            config.set_parameter("automatically_purge", True)
            sqlite.set_config(config)
        except Exception as e:
            console.print(f"[bold red]Error vacuuming database:[/bold red] {e}")
            raise typer.Exit(code=1)

        progress.update(task, advance=100)

    directory_size_after_vacuum = get_directory_size(path)
    size_diff = directory_size_before_vacuum - directory_size_after_vacuum

    console.print(
        f":soap: [bold]vacuum complete![/bold] Database size reduced by [green]{sizeof_fmt(size_diff)}[/green] (:arrow_down: [bold green]{(size_diff * 100 / directory_size_before_vacuum):.1f}%[/bold green])."
    )


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
