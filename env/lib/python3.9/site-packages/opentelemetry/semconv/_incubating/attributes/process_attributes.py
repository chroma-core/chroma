# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum
from typing import Final

from deprecated import deprecated

PROCESS_COMMAND: Final = "process.command"
"""
The command used to launch the process (i.e. the command name). On Linux based systems, can be set to the zeroth string in `proc/[pid]/cmdline`. On Windows, can be set to the first parameter extracted from `GetCommandLineW`.
"""

PROCESS_COMMAND_ARGS: Final = "process.command_args"
"""
All the command arguments (including the command/executable itself) as received by the process. On Linux-based systems (and some other Unixoid systems supporting procfs), can be set according to the list of null-delimited strings extracted from `proc/[pid]/cmdline`. For libc-based executables, this would be the full argv vector passed to `main`.
"""

PROCESS_COMMAND_LINE: Final = "process.command_line"
"""
The full command used to launch the process as a single string representing the full command. On Windows, can be set to the result of `GetCommandLineW`. Do not set this if you have to assemble it just for monitoring; use `process.command_args` instead.
"""

PROCESS_CONTEXT_SWITCH_TYPE: Final = "process.context_switch_type"
"""
Specifies whether the context switches for this data point were voluntary or involuntary.
"""

PROCESS_CPU_STATE: Final = "process.cpu.state"
"""
Deprecated: Replaced by `cpu.mode`.
"""

PROCESS_CREATION_TIME: Final = "process.creation.time"
"""
The date and time the process was created, in ISO 8601 format.
"""

PROCESS_EXECUTABLE_NAME: Final = "process.executable.name"
"""
The name of the process executable. On Linux based systems, can be set to the `Name` in `proc/[pid]/status`. On Windows, can be set to the base name of `GetProcessImageFileNameW`.
"""

PROCESS_EXECUTABLE_PATH: Final = "process.executable.path"
"""
The full path to the process executable. On Linux based systems, can be set to the target of `proc/[pid]/exe`. On Windows, can be set to the result of `GetProcessImageFileNameW`.
"""

PROCESS_EXIT_CODE: Final = "process.exit.code"
"""
The exit code of the process.
"""

PROCESS_EXIT_TIME: Final = "process.exit.time"
"""
The date and time the process exited, in ISO 8601 format.
"""

PROCESS_GROUP_LEADER_PID: Final = "process.group_leader.pid"
"""
The PID of the process's group leader. This is also the process group ID (PGID) of the process.
"""

PROCESS_INTERACTIVE: Final = "process.interactive"
"""
Whether the process is connected to an interactive shell.
"""

PROCESS_OWNER: Final = "process.owner"
"""
The username of the user that owns the process.
"""

PROCESS_PAGING_FAULT_TYPE: Final = "process.paging.fault_type"
"""
The type of page fault for this data point. Type `major` is for major/hard page faults, and `minor` is for minor/soft page faults.
"""

PROCESS_PARENT_PID: Final = "process.parent_pid"
"""
Parent Process identifier (PPID).
"""

PROCESS_PID: Final = "process.pid"
"""
Process identifier (PID).
"""

PROCESS_REAL_USER_ID: Final = "process.real_user.id"
"""
The real user ID (RUID) of the process.
"""

PROCESS_REAL_USER_NAME: Final = "process.real_user.name"
"""
The username of the real user of the process.
"""

PROCESS_RUNTIME_DESCRIPTION: Final = "process.runtime.description"
"""
An additional description about the runtime of the process, for example a specific vendor customization of the runtime environment.
"""

PROCESS_RUNTIME_NAME: Final = "process.runtime.name"
"""
The name of the runtime of this process.
"""

PROCESS_RUNTIME_VERSION: Final = "process.runtime.version"
"""
The version of the runtime of this process, as returned by the runtime without modification.
"""

PROCESS_SAVED_USER_ID: Final = "process.saved_user.id"
"""
The saved user ID (SUID) of the process.
"""

PROCESS_SAVED_USER_NAME: Final = "process.saved_user.name"
"""
The username of the saved user.
"""

PROCESS_SESSION_LEADER_PID: Final = "process.session_leader.pid"
"""
The PID of the process's session leader. This is also the session ID (SID) of the process.
"""

PROCESS_USER_ID: Final = "process.user.id"
"""
The effective user ID (EUID) of the process.
"""

PROCESS_USER_NAME: Final = "process.user.name"
"""
The username of the effective user of the process.
"""

PROCESS_VPID: Final = "process.vpid"
"""
Virtual process identifier.
Note: The process ID within a PID namespace. This is not necessarily unique across all processes on the host but it is unique within the process namespace that the process exists within.
"""


class ProcessContextSwitchTypeValues(Enum):
    VOLUNTARY = "voluntary"
    """voluntary."""
    INVOLUNTARY = "involuntary"
    """involuntary."""


@deprecated(reason="The attribute process.cpu.state is deprecated - Replaced by `cpu.mode`")  # type: ignore
class ProcessCpuStateValues(Enum):
    SYSTEM = "system"
    """system."""
    USER = "user"
    """user."""
    WAIT = "wait"
    """wait."""


class ProcessPagingFaultTypeValues(Enum):
    MAJOR = "major"
    """major."""
    MINOR = "minor"
    """minor."""
