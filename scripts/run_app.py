import os
import pathlib
import subprocess
import time

from chroma.cli.multi_command import MultiCommand, SubCommand
from shared_runners import app_subcommand

if __name__ == "__main__":
    # os.getcwd will give the directory wherever the CLI is called
    # so we have to do this instead
    base_dir = str(pathlib.Path(__file__).parent.parent.resolve())

    multicommand = MultiCommand()

    app_sub_command = app_subcommand(base_dir, multicommand)
    multicommand.append_threaded_command(app_sub_command.name, app_sub_command)

    multicommand.run()
