import typer
import os
import time
import subprocess
from todoer.cli_commands.multi_command import MultiCommand, SubCommand
import pathlib

typer_app = typer.Typer()

@typer_app.command()
def run():
    typer.echo("Running application...")
    # base_dir = os.getcwd() 
    base_dir = str(pathlib.Path(__file__).parent.parent.resolve())

    multicommand = MultiCommand()

    backend_env = os.environ.copy()
    backend_env["FLASK_APP"] = "main.py"
    backend_env["FLASK_ENV"] = "development"
    backend_directory = "/".join((base_dir, 'backend'))
    multicommand.append_threaded_command("webserver", SubCommand(
        multicommand,
        name="webserver",
        command=['flask run'],
        env=backend_env,
        cwd=backend_directory
    ))
    
    # frontend_directory = "/".join((base_dir, 'frontend'))
    # multicommand.append_threaded_command("frontend", SubCommand(
    #     multicommand,
    #     name="frontend",
    #     command=['yarn start'],
    #     cwd=frontend_directory
    # ))

    multicommand.run()

if __name__ == "__main__":
    typer_app()