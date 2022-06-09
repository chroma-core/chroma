import typer
import os
import time
import subprocess
from chroma.cli_commands.multi_command import MultiCommand, SubCommand
import pathlib

typer_app = typer.Typer()

@typer_app.command()
def run():
    typer.echo("Running application...")
    
    # os.getcwd will give the directory wherever the CLI is called
    # so we have to do this instead 
    base_dir = str(pathlib.Path(__file__).parent.parent.resolve())

    multicommand = MultiCommand()

    backend_env = os.environ.copy()
    backend_env["FLASK_APP"] = "main.py"
    backend_env["FLASK_ENV"] = "development"
    backend_directory = "/".join((base_dir, 'app_backend'))
    multicommand.append_threaded_command("webserver", SubCommand(
        multicommand,
        name="webserver",
        command=['flask run'],
        env=backend_env,
        cwd=backend_directory
    ))
  
    multicommand.run()

if __name__ == "__main__":
    typer_app()