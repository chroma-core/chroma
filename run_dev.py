import os
import pathlib
import subprocess
import time

from chroma.cli_commands.multi_command import MultiCommand, SubCommand

if __name__ == "__main__":
    # os.getcwd will give the directory wherever the CLI is called
    # so we have to do this instead
    base_dir = str(pathlib.Path(__file__).parent.parent.resolve())

    multicommand = MultiCommand()

    data_manager_env = os.environ.copy()
    data_manager_env["FLASK_APP"] = "main.py"
    data_manager_env["FLASK_ENV"] = "development"
    data_manager_directory = "/".join((base_dir, "chroma/data_manager"))
    multicommand.append_threaded_command(
        "Data Manager",
        SubCommand(
            multicommand,
            name="Data Manager",
            command=["flask run --port 5000"],
            env=data_manager_env,
            cwd=data_manager_directory,
            ready_string="Debugger is active!",
        ),
    )

    app_backend_env = os.environ.copy()
    app_backend_env["FLASK_APP"] = "main.py"
    app_backend_env["FLASK_ENV"] = "development"
    app_backend_directory = "/".join((base_dir, "chroma/app_backend"))
    multicommand.append_threaded_command(
        "App Backend",
        SubCommand(
            multicommand,
            name="App Backend",
            command=["flask run --port 4000"],
            env=app_backend_env,
            cwd=app_backend_directory,
            ready_string="finished processing datapoints",
        ),
    )

    app_frontend_env = os.environ.copy()
    app_frontend_directory = "/".join((base_dir, "chroma-ui"))
    multicommand.append_threaded_command(
        "Frontend",
        SubCommand(
            multicommand,
            name="Frontend",
            command=["yarn start"],
            env=app_frontend_env,
            cwd=app_frontend_directory,
            ready_string="No issues found",
        ),
    )

    multicommand.run()
