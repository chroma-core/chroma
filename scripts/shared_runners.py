import os
from chroma.cli_commands.multi_command import SubCommand

def app_subcommand(base_dir, multicommand):
    app_env = os.environ.copy()
    app_directory = "/".join((base_dir, "chroma/app"))

    subcommand = SubCommand(
        multicommand,
        name="App",
        command=["uvicorn app:app --reload --host '::'"],
        env=app_env,
        cwd=app_directory,
        ready_string="Application startup complete",
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
