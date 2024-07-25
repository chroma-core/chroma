from typing import Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
import typer.rich_utils
from typing_extensions import Annotated
import typer
import uvicorn
import os
import webbrowser

from chromadb.cli.utils import get_directory_size, set_log_file_path, sizeof_fmt
from chromadb.config import Settings, System
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.ingest.impl.utils import trigger_vector_segments_max_seq_id_migration
from chromadb.segment import SegmentManager

app = typer.Typer()
utils_app = typer.Typer(short_help="Use maintenance utilities")
app.add_typer(utils_app, name="utils")

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
    host: Annotated[
        Optional[str], typer.Option(help="The host to listen to. Default: localhost")
    ] = "localhost",
    log_path: Annotated[
        Optional[str], typer.Option(help="The path to the log file.")
    ] = "chroma.log",
    port: int = typer.Option(8000, help="The port to run the server on."),
    test: bool = typer.Option(False, help="Test mode.", show_envvar=False, hidden=True),
) -> None:
    """Run a chroma server"""
    console = Console()

    print("\033[1m")  # Bold logo
    print(_logo)
    print("\033[1m")  # Bold
    print("Running Chroma")
    print("\033[0m")  # Reset

    console.print(f"[bold]Saving data to:[/bold] [green]{path}[/green]")
    console.print(
        f"[bold]Connect to chroma at:[/bold] [green]http://{host}:{port}[/green]"
    )
    console.print(
        "[bold]Getting started guide[/bold]: [blue]https://docs.trychroma.com/getting-started[/blue]\n\n"
    )

    # set ENV variable for PERSIST_DIRECTORY to path
    os.environ["IS_PERSISTENT"] = "True"
    os.environ["PERSIST_DIRECTORY"] = path
    os.environ["CHROMA_SERVER_NOFILE"] = "65535"
    os.environ["CHROMA_CLI"] = "True"

    # get the path where chromadb is installed
    chromadb_path = os.path.dirname(os.path.realpath(__file__))

    # this is the path of the CLI, we want to move up one directory
    chromadb_path = os.path.dirname(chromadb_path)
    log_config = set_log_file_path(f"{chromadb_path}/log_config.yml", f"{log_path}")
    config = {
        "app": "chromadb.app:app",
        "host": host,
        "port": port,
        "workers": 1,
        "log_config": log_config,  # Pass the modified log_config dictionary
        "timeout_keep_alive": 30,
    }

    if test:
        return

    uvicorn.run(**config)


@utils_app.command()  # type: ignore
def vacuum(
    path: str = typer.Option(
        help="The path to a Chroma data directory.",
    ),
    force: bool = typer.Option(False, help="Force vacuuming without confirmation."),
) -> None:
    """
    Vacuum the database. This may result in a small increase in performance.

    If you recently upgraded Chroma from a version below 0.6 to 0.6 or above, you should run this command once to greatly reduce the size of your database and enable continuous database pruning. In most other cases, vacuuming will save very little disk space.

    The execution time of this command scales with the size of your database. It block both reads and writes to the database while it is running.
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
        "Are you sure you want to vacuum the database? This will block both reads and writes to the database and may take a while."
    ):
        console.print("Vacuum cancelled.")
        raise typer.Exit(code=0)

    settings = Settings()
    settings.is_persistent = True
    settings.persist_directory = path
    system = System(settings=settings)
    sqlite = system.instance(SqliteDB)

    directory_size_before_vacuum = get_directory_size(path)

    console.print()  # Add a newline before the progress bar

    with Progress(
        SpinnerColumn(finished_text="[bold green]:heavy_check_mark:[/bold green]"),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task = progress.add_task("Pruning the log...")
        try:
            # Cleaning the log after upgrading to >=0.6 is dependent on vector segments migrating their max_seq_id from the pickled metadata file to SQLite.
            # Vector segments migrate this field automatically on init, but at this point the segment has not been loaded yet.
            trigger_vector_segments_max_seq_id_migration(
                sqlite, system.instance(SegmentManager)
            )
            sqlite.purge_log()
        except Exception as e:
            console.print(f"[bold red]Error pruning the log:[/bold red] {e}")
            raise typer.Exit(code=1)
        progress.update(task, advance=100)

        task = progress.add_task("Vacuuming (this may take a while)...")
        try:
            sqlite.vacuum()
            config = sqlite.config
            config.set_parameter("automatically_prune", True)
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
