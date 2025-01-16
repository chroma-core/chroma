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

CICD_PIPELINE_NAME: Final = "cicd.pipeline.name"
"""
The human readable name of the pipeline within a CI/CD system.
"""

CICD_PIPELINE_RUN_ID: Final = "cicd.pipeline.run.id"
"""
The unique identifier of a pipeline run within a CI/CD system.
"""

CICD_PIPELINE_TASK_NAME: Final = "cicd.pipeline.task.name"
"""
The human readable name of a task within a pipeline. Task here most closely aligns with a [computing process](https://en.wikipedia.org/wiki/Pipeline_(computing)) in a pipeline. Other terms for tasks include commands, steps, and procedures.
"""

CICD_PIPELINE_TASK_RUN_ID: Final = "cicd.pipeline.task.run.id"
"""
The unique identifier of a task run within a pipeline.
"""

CICD_PIPELINE_TASK_RUN_URL_FULL: Final = "cicd.pipeline.task.run.url.full"
"""
The [URL](https://en.wikipedia.org/wiki/URL) of the pipeline run providing the complete address in order to locate and identify the pipeline run.
"""

CICD_PIPELINE_TASK_TYPE: Final = "cicd.pipeline.task.type"
"""
The type of the task within a pipeline.
"""


class CicdPipelineTaskTypeValues(Enum):
    BUILD = "build"
    """build."""
    TEST = "test"
    """test."""
    DEPLOY = "deploy"
    """deploy."""
