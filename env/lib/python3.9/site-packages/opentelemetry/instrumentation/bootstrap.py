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

import argparse
import logging
import sys
from subprocess import (
    PIPE,
    CalledProcessError,
    Popen,
    SubprocessError,
    check_call,
)

import pkg_resources

from opentelemetry.instrumentation.bootstrap_gen import (
    default_instrumentations,
    libraries,
)
from opentelemetry.instrumentation.version import __version__

logger = logging.getLogger(__name__)


def _syscall(func):
    def wrapper(package=None):
        try:
            if package:
                return func(package)
            return func()
        except SubprocessError as exp:
            cmd = getattr(exp, "cmd", None)
            if cmd:
                msg = f'Error calling system command "{" ".join(cmd)}"'
            if package:
                msg = f'{msg} for package "{package}"'
            raise RuntimeError(msg)

    return wrapper


@_syscall
def _sys_pip_install(package):
    # explicit upgrade strategy to override potential pip config
    try:
        check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "-U",
                "--upgrade-strategy",
                "only-if-needed",
                package,
            ]
        )
    except CalledProcessError as error:
        print(error)


def _pip_check():
    """Ensures none of the instrumentations have dependency conflicts.
    Clean check reported as:
    'No broken requirements found.'
    Dependency conflicts are reported as:
    'opentelemetry-instrumentation-flask 1.0.1 has requirement opentelemetry-sdk<2.0,>=1.0, but you have opentelemetry-sdk 0.5.'
    To not be too restrictive, we'll only check for relevant packages.
    """
    with Popen(
        [sys.executable, "-m", "pip", "check"], stdout=PIPE
    ) as check_pipe:
        pip_check = check_pipe.communicate()[0].decode()
        pip_check_lower = pip_check.lower()
    for package_tup in libraries:
        for package in package_tup:
            if package.lower() in pip_check_lower:
                raise RuntimeError(f"Dependency conflict found: {pip_check}")


def _is_installed(req):
    if req in sys.modules:
        return True

    try:
        pkg_resources.get_distribution(req)
    except pkg_resources.DistributionNotFound:
        return False
    except pkg_resources.VersionConflict as exc:
        logger.warning(
            "instrumentation for package %s is available but version %s is installed. Skipping.",
            exc.req,
            exc.dist.as_requirement(),  # pylint: disable=no-member
        )
        return False
    return True


def _find_installed_libraries():
    for lib in default_instrumentations:
        yield lib

    for lib in libraries:
        if _is_installed(lib["library"]):
            yield lib["instrumentation"]


def _run_requirements():
    logger.setLevel(logging.ERROR)
    print("\n".join(_find_installed_libraries()))


def _run_install():
    for lib in _find_installed_libraries():
        _sys_pip_install(lib)
    _pip_check()


def run() -> None:
    action_install = "install"
    action_requirements = "requirements"

    parser = argparse.ArgumentParser(
        description="""
        opentelemetry-bootstrap detects installed libraries and automatically
        installs the relevant instrumentation packages for them.
        """
    )
    parser.add_argument(
        "--version",
        help="print version information",
        action="version",
        version="%(prog)s " + __version__,
    )
    parser.add_argument(
        "-a",
        "--action",
        choices=[action_install, action_requirements],
        default=action_requirements,
        help="""
        install - uses pip to install the new requirements using to the
                  currently active site-package.
        requirements - prints out the new requirements to stdout. Action can
                       be piped and appended to a requirements.txt file.
        """,
    )
    args = parser.parse_args()

    cmd = {
        action_install: _run_install,
        action_requirements: _run_requirements,
    }[args.action]
    cmd()
