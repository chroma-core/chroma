import os
import pathlib
import subprocess
import time

import typer

from chroma.cli.multi_command import MultiCommand, SubCommand

typer_app = typer.Typer()


@typer_app.command()
def run():
    typer.echo("Running application...")

    # os.getcwd will give the directory wherever the CLI is called
    # so we have to do this instead
    base_dir = str(pathlib.Path(__file__).parent.parent.resolve())

    multicommand = MultiCommand()

    app_env = os.environ.copy()
    app_directory = "/".join((base_dir, "app"))

    subcommand = SubCommand(
        multicommand,
        name="App",
        command=["uvicorn app:app --reload --host '::'"],
        env=app_env,
        cwd=app_directory,
        ready_string="Application startup complete",
    )
    multicommand.append_threaded_command(subcommand.name, subcommand)

    multicommand.run()


if __name__ == "__main__":
    typer_app()
