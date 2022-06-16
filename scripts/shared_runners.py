import os
from chroma.cli_commands.multi_command import SubCommand


def data_manager_subcommand(base_dir, multicommand):
    data_manager_env = os.environ.copy()
    data_manager_env["FLASK_APP"] = "main.py"
    data_manager_env["FLASK_ENV"] = "development"
    data_manager_directory = "/".join((base_dir, "chroma/data_manager"))

    subcommand = SubCommand(
        multicommand,
        name="Data Manager",
        command=["flask run --port 5000"],
        env=data_manager_env,
        cwd=data_manager_directory,
        ready_string="Debugger is active!",
    )
    return subcommand


def app_backend_subcommand(base_dir, multicommand):
    app_backend_env = os.environ.copy()
    app_backend_env["FLASK_APP"] = "main.py"
    app_backend_env["FLASK_ENV"] = "development"
    app_backend_directory = "/".join((base_dir, "chroma/app_backend"))

    subcommand = SubCommand(
        multicommand,
        name="App Backend",
        command=["flask run --port 4000"],
        env=app_backend_env,
        cwd=app_backend_directory,
        ready_string="finished processing datapoints",
    )
    return subcommand


def frontend_subcommand(base_dir, multicommand):
    app_frontend_env = os.environ.copy()
    app_frontend_directory = "/".join((base_dir, "chroma-ui"))

    subcommand = SubCommand(
        multicommand,
        name="Frontend",
        command=["yarn start"],
        env=app_frontend_env,
        cwd=app_frontend_directory,
        ready_string="No issues found",
    )
    return subcommand
