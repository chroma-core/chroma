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

from logging import getLogger
from os import environ
from os.path import abspath, dirname, pathsep

from opentelemetry.instrumentation.auto_instrumentation._load import (
    _load_configurators,
    _load_distro,
    _load_instrumentors,
)
from opentelemetry.instrumentation.utils import _python_path_without_directory

logger = getLogger(__name__)


def initialize():
    # prevents auto-instrumentation of subprocesses if code execs another python process
    environ["PYTHONPATH"] = _python_path_without_directory(
        environ["PYTHONPATH"], dirname(abspath(__file__)), pathsep
    )

    try:
        distro = _load_distro()
        distro.configure()
        _load_configurators()
        _load_instrumentors(distro)
    except Exception:  # pylint: disable=broad-except
        logger.exception("Failed to auto initialize opentelemetry")


initialize()
