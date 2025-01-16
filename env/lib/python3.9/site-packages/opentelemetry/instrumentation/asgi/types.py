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

from typing import Any, Callable, Dict, Optional

from opentelemetry.trace import Span

_Scope = Dict[str, Any]
_Message = Dict[str, Any]

ServerRequestHook = Optional[Callable[[Span, _Scope], None]]
"""
Incoming request callback type.

Args:
    - Server span
    - ASGI scope as a mapping
"""

ClientRequestHook = Optional[Callable[[Span, _Scope, _Message], None]]
"""
Receive callback type.

Args:
    - Internal span
    - ASGI scope as a mapping
    - ASGI event as a mapping
"""

ClientResponseHook = Optional[Callable[[Span, _Scope, _Message], None]]
"""
Send callback type.

Args:
    - Internal span
    - ASGI scope as a mapping
    - ASGI event as a mapping
"""
