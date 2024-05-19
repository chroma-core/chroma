from typing import Optional

from typing_extensions import Annotated
import typer
from click.core import ParameterSource
import uvicorn
import os
import webbrowser

from chromadb.cli.utils import set_log_file_path

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
    ctx: typer.Context,
    path: str = typer.Option(
        "./chroma_data", help="The path to the file or directory."
    ),
    persistent: Annotated[
        Optional[bool], typer.Option(help="If set, the server will run in persistent mode.")
    ] = True,
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

    print("\033[1m")  # Bold logo
    print(_logo)
    print("\033[1m")  # Bold
    print("Running Chroma")
    print("\033[0m")  # Reset

    if persistent:
        typer.echo(f"\033[1mSaving data to\033[0m: \033[32m{path}\033[0m")
    else:
        if ctx.get_parameter_source("path")!=ParameterSource.DEFAULT:
            typer.echo("You can't set the path parameter while using in-memory mode.")
            raise typer.Abort()

        typer.echo(f"\033[1mRunning in \033[32min-memory mode\033[0m\033[1m,\u001b[31m all changes to the database will be discarded after the application exits.\033[0m")

    typer.echo(
        f"\033[1mConnect to chroma at\033[0m: \033[32mhttp://{host}:{port}\033[0m"
    )
    typer.echo(
        "\033[1mGetting started guide\033[0m: https://docs.trychroma.com/getting-started\n\n"
    )

    # set ENV variable for PERSIST_DIRECTORY to path
    os.environ["IS_PERSISTENT"] = str(persistent)
    os.environ["PERSIST_DIRECTORY"] = path
    os.environ["CHROMA_SERVER_NOFILE"] = "65535"

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
