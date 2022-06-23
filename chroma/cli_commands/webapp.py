import os
import pathlib
import subprocess
import time

import typer

from chroma.cli_commands.multi_command import MultiCommand, SubCommand

typer_app = typer.Typer()


@typer_app.command()
def run():
    typer.echo("Running application...")

    # os.getcwd will give the directory wherever the CLI is called
    # so we have to do this instead
    base_dir = str(pathlib.Path(__file__).parent.parent.resolve())

    multicommand = MultiCommand()

    data_manager_env = os.environ.copy()
    data_manager_env["FLASK_APP"] = "main.py"
    data_manager_env["FLASK_ENV"] = "production"
    data_manager_directory = "/".join((base_dir, "data_manager"))

    subcommand = SubCommand(
        multicommand,
        name="Data Manager",
        command=["flask run --port 5000"],
        env=data_manager_env,
        cwd=data_manager_directory,
        ready_string="Running on http://127.0.0.1:5000/",
    )
    multicommand.append_threaded_command(subcommand.name, subcommand)

    app_backend_env = os.environ.copy()
    app_backend_env["FLASK_APP"] = "main.py"
    app_backend_env["FLASK_ENV"] = "production"
    app_backend_directory = "/".join((base_dir, "app_backend"))

    subcommand = SubCommand(
        multicommand,
        name="App Backend",
        command=["flask run --port 4000"],
        env=app_backend_env,
        cwd=app_backend_directory,
        ready_string="Running on http://127.0.0.1:4000/",
    )
    multicommand.append_threaded_command(subcommand.name, subcommand)

    multicommand.run()


if __name__ == "__main__":
    typer_app()
