import logging
import os
import random
import socket
import subprocess
import threading
import time
from collections import deque
from termcolor import colored
from typing import Dict, List

class MultiCommand:
    """
    Runs all components of Todoer under a single parent process.
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
        self.print_output("alert", "appending serial command")
        self.serial_subcommands[name] = command

    def append_threaded_command(self, name, command):
        """Add a threaded command before runnning run"""
        self.print_output("alert", "appending threaded command")
        self.threaded_subcommands[name] = command

    def run(self):
        """Main run loop"""
        self.print_output("Todoer", "Starting ")
        
        # Silence built-in logging at INFO
        logging.getLogger("").setLevel(logging.WARNING)
        
        # TODO: add serial command execution
        for command in self.serial_subcommands.values():
            command()

        # Run subcommand threads
        for command in self.threaded_subcommands.values():
            command.start()

        # Run output loop
        shown_ready = False
        while True:
            try:
                # Print all the current lines onto the screen
                self.update_output()
                # Print info banner when all components are ready and the
                # delay has passed
                if not self.ready_time and self.is_ready():
                    self.ready_time = time.monotonic()
                if (
                    not shown_ready
                    and self.ready_time
                    and time.monotonic() - self.ready_time > self.ready_delay
                ):
                    self.print_ready()
                    shown_ready = True
                # Ensure we idle-sleep rather than fast-looping
                time.sleep(0.1)
            except KeyboardInterrupt:
                break

        # Stop subcommand threads
        self.print_output("Todoer", "Shutting down components")
        for command in self.threaded_subcommands.values():
            command.stop()
        for command in self.threaded_subcommands.values():
            command.join()
        self.print_output("Todoer", "Complete")

    def update_output(self):
        """Drains the output queue and prints its contents to the screen"""
        while self.output_queue:
            name, line = self.output_queue.popleft() # Extract info
            line_str = line.decode("utf8").strip() # Make line printable
            self.print_output(name, line_str)

    def print_output(self, name: str, output):
        """
        Prints an output line with name and colouring. You can pass multiple
        lines to output if you wish; it will be split for you.
        """
        color = {
            "alert": "red",
            "scheduler": "blue",
            "triggerer": "cyan",
            "Todoer": "white",
        }.get(name, "white")
        colorised_name = colored("%10s" % name, color)
        for line in output.split("\n"):
            print(f"{colorised_name} | {line.strip()}")

    def print_error(self, name: str, output):
        """
        Prints an error message to the console (this is the same as
        print_output but with the text red)
        """
        self.print_output(name, colored(output, "red"))

    # def initialize_database(self):
    #     """Makes sure all the tables are created."""
    #     # Set up DB tables
    #     self.print_output("Todoer", "Setting up DB")

    #     base_dir = cwd = os.getcwd() 
    #     backend_directory = "/".join((base_dir, 'backend'))

    #     self.process = subprocess.Popen(
    #         'python setup_db.py',
    #         stdout=subprocess.PIPE,
    #         stderr=subprocess.STDOUT,
    #         cwd = backend_directory,
    #         shell=True
    #     )

    #     for line in self.process.stdout:
    #         self.parent.output_queue.append((self.name, line))

    #     self.print_output("Todoer", "Database setup")

    def is_ready(self):
        """
        Detects when all Todoer components are ready to serve.
        For now, it's simply time-based.
        """
        return (
            self.port_open(self.web_server_port)
        )

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
        except (OSError, ValueError): # Any exception means the socket is not available
            return False
        return True

    def print_ready(self):
        """
        Prints the banner shown when Todoer is ready to go
        """
        self.print_output("Todoer", "")
        self.print_output("Todoer", "Todoer is ready")
        self.print_output(
            "Todoer",
            "Todoer threaded is for development purposes only. Do not use this in production!",
        )
        self.print_output("Todoer", "")


class SubCommand(threading.Thread):
    """
    Thread that launches a process and then streams its output back to the main
    command. We use threads to avoid using select() and raw filehandles, and the
    complex logic that brings doing line buffering.
    """

    def __init__(self, parent, name: str, command: List[str], env: Dict[str, str] = None, cwd:str = None):
        super().__init__()
        self.parent = parent
        self.name = name
        self.command = command
        self.env = env if env else os.environ.copy()
        self.cwd = cwd if cwd else os.getcwd()

    def run(self):
        """Runs the actual process and captures it output to a queue"""
        self.process = subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd = self.cwd,
            env=self.env,
            shell=True,
        )
        for line in self.process.stdout:
            self.parent.output_queue.append((self.name, line))

    def stop(self):
        """Call to stop this process (and thus this thread)"""
        self.process.terminate()
