import os
import pathlib
import subprocess
import time

from chroma.cli_commands.multi_command import MultiCommand, SubCommand
from shared_runners import data_manager_subcommand, app_backend_subcommand, frontend_subcommand

if __name__ == "__main__":
    # os.getcwd will give the directory wherever the CLI is called
    # so we have to do this instead
    base_dir = str(pathlib.Path(__file__).parent.parent.resolve())

    multicommand = MultiCommand()

    app_backend_sub_command = app_backend_subcommand(base_dir, multicommand)
    multicommand.append_threaded_command(app_backend_sub_command.name, app_backend_sub_command)

    multicommand.run()
