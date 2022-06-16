# heavily based on https://github.com/apache/airflow/blob/main/airflow/cli/commands/standalone_command.py

import logging
import os
import random
import socket
import subprocess
import threading
import time
from collections import deque
from typing import Dict, List
import inspect

chroma_logo = """
 ______     __  __     ______     ______     __    __     ______       
/\  ___\   /\ \_\ \   /\  == \   /\  __ \   /\ "-./  \   /\  __ \      
\ \ \____  \ \  __ \  \ \  __<   \ \ \/\ \  \ \ \-./\ \  \ \  __ \     
  \ \_____\  \ \_\ \_\  \ \_\ \_\  \ \_____\  \ \_\ \ \_\  \ \_\ \_\    
   \/_____/   \/_/\/_/   \/_/ /_/   \/_____/   \/_/  \/_/   \/_/\/_/    
                                                                       
"""


class MultiCommand:
    """
    Runs all components of Chroma under a single parent process.
    Useful for local development.
    """

    def __init__(self):
        # commands to run in serial
        self.serial_subcommands = {}

        # commands to run in parellel
        self.threaded_subcommands = {}

        # log queue
        self.output_queue = deque()

        # timers
        self.ready_time = None
        self.ready_delay = 3

        # settings
        self.web_server_port = 5000

    def append_serial_command(self, name, command):
        """Add a serial command before runnning run"""
        # self.print_output("alert", "appending serial command")
        self.serial_subcommands[name] = command

    def append_threaded_command(self, name, command):
        """Add a threaded command before runnning run"""
        # self.print_output("alert", "appending threaded command")
        self.threaded_subcommands[name] = command

    def run(self):
        """Main run loop"""

        self.print_output_no_strip("Chroma", chroma_logo)
        self.print_output("Chroma", "Starting ")

        # Silence built-in logging at INFO
        logging.getLogger("").setLevel(logging.WARNING)

        # # TODO: add serial command execution
        # for command in self.serial_subcommands.values():
        #     command()

        # Run subcommand threads
        # for command in self.threaded_subcommands.values():
        #     command.start()

        # start first command
        currentCommand = 0
        commandList = list(self.threaded_subcommands.values())
        commandListLength = len(commandList)
        commandList[currentCommand].start()

        # Run output loop
        shown_ready = False
        while True:
            try:
                # Print all the current lines onto the screen
                self.update_output()

                # if we have finished our current command and there is a next command
                if commandList[currentCommand].isReady and (
                    (currentCommand + 1) < commandListLength
                ):
                    currentCommand = currentCommand + 1
                    commandList[currentCommand].start()

                # print done info banner when everything is ready
                if (
                    commandList[currentCommand].isReady
                    and ((currentCommand + 1) == commandListLength)
                    and not shown_ready
                ):
                    self.print_ready()
                    shown_ready = True
                # Ensure we idle-sleep rather than fast-looping
                # time.sleep(0.1)
            except KeyboardInterrupt:
                break

        # Stop subcommand threads
        self.print_output("Chroma", "Shutting down components")
        for command in self.threaded_subcommands.values():
            command.stop()
        for command in self.threaded_subcommands.values():
            command.join()
        self.print_output("Chroma", "Complete")

    def update_output(self):
        """Drains the output queue and prints its contents to the screen"""
        while self.output_queue:
            name, line_str, isReady = self.output_queue.popleft()  # Extract info
            self.print_output(name, line_str)

    def print_output(self, name: str, output):
        """
        Prints an output line with name and colouring. You can pass multiple
        lines to output if you wish; it will be split for you.
        """
        for line in output.split("\n"):
            print(f"{name} | {line.strip()}")

    def print_output_no_strip(self, name: str, output):
        """
        Prints an output line with name and colouring. You can pass multiple
        lines to output if you wish; it will be split for you. This variant
        does not remove whitespace, which we want to display our ASCII correctly
        """
        for line in output.split("\n"):
            print(f"{name} | {line}")

    def print_error(self, name: str, output):
        """
        Prints an error message to the console (this is the same as
        print_output but with the text red)
        """
        self.print_output(name, output)

    def is_ready(self):
        """
        Detects when all Chroma components are ready to serve.
        For now, it's simply time-based.
        """
        return self.port_open(self.web_server_port)

    def port_open(self, port):
        """
        Checks if the given port is listening on the local machine.
        (used to tell if webserver is alive)
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect(("127.0.0.1", port))
            sock.close()
        except (OSError, ValueError):  # Any exception means the socket is not available
            return False
        return True

    def print_ready(self):
        """
        Prints the banner shown when Chroma is ready to go
        """
        self.print_output("Chroma", "")
        self.print_output("Chroma", "Chroma is ready")
        self.print_output(
            "Chroma",
            "Please open http://localhost:4000 (Command + Click on Mac)",
        )
        self.print_output("Chroma", "")


class SubCommand(threading.Thread):
    """
    Thread that launches a process and then streams its output back to the main
    command. We use threads to avoid using select() and raw filehandles, and the
    complex logic that brings doing line buffering.
    """

    def __init__(
        self,
        parent,
        name: str,
        command: List[str],
        env: Dict[str, str] = None,
        cwd: str = None,
        ready_string: str = None,
    ):
        super().__init__()
        self.parent = parent
        self.name = name
        self.command = command
        self.env = env if env else os.environ.copy()
        self.cwd = cwd if cwd else os.getcwd()
        self.ready_string = ready_string if ready_string else None
        self.isReady = False

    def run(self):
        """Runs the actual process and captures it output to a queue"""
        self.process = subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self.cwd,
            env=self.env,
            shell=True,
        )
        for line in self.process.stdout:
            line_str = line.decode("utf8").strip()
            if self.ready_string in line_str:
                self.isReady = True
            self.parent.output_queue.append((self.name, line_str, self.isReady))

    def stop(self):
        """Call to stop this process (and thus this thread)"""
        self.process.terminate()
