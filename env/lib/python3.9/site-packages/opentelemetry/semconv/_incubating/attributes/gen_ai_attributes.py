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

GEN_AI_COMPLETION: Final = "gen_ai.completion"
"""
The full response received from the GenAI model.
Note: It's RECOMMENDED to format completions as JSON string matching [OpenAI messages format](https://platform.openai.com/docs/guides/text-generation).
"""

GEN_AI_OPERATION_NAME: Final = "gen_ai.operation.name"
"""
The name of the operation being performed.
Note: If one of the predefined values applies, but specific system uses a different name it's RECOMMENDED to document it in the semantic conventions for specific GenAI system and use system-specific name in the instrumentation. If a different name is not documented, instrumentation libraries SHOULD use applicable predefined value.
"""

GEN_AI_PROMPT: Final = "gen_ai.prompt"
"""
The full prompt sent to the GenAI model.
Note: It's RECOMMENDED to format prompts as JSON string matching [OpenAI messages format](https://platform.openai.com/docs/guides/text-generation).
"""

GEN_AI_REQUEST_FREQUENCY_PENALTY: Final = "gen_ai.request.frequency_penalty"
"""
The frequency penalty setting for the GenAI request.
"""

GEN_AI_REQUEST_MAX_TOKENS: Final = "gen_ai.request.max_tokens"
"""
The maximum number of tokens the model generates for a request.
"""

GEN_AI_REQUEST_MODEL: Final = "gen_ai.request.model"
"""
The name of the GenAI model a request is being made to.
"""

GEN_AI_REQUEST_PRESENCE_PENALTY: Final = "gen_ai.request.presence_penalty"
"""
The presence penalty setting for the GenAI request.
"""

GEN_AI_REQUEST_STOP_SEQUENCES: Final = "gen_ai.request.stop_sequences"
"""
List of sequences that the model will use to stop generating further tokens.
"""

GEN_AI_REQUEST_TEMPERATURE: Final = "gen_ai.request.temperature"
"""
The temperature setting for the GenAI request.
"""

GEN_AI_REQUEST_TOP_K: Final = "gen_ai.request.top_k"
"""
The top_k sampling setting for the GenAI request.
"""

GEN_AI_REQUEST_TOP_P: Final = "gen_ai.request.top_p"
"""
The top_p sampling setting for the GenAI request.
"""

GEN_AI_RESPONSE_FINISH_REASONS: Final = "gen_ai.response.finish_reasons"
"""
Array of reasons the model stopped generating tokens, corresponding to each generation received.
"""

GEN_AI_RESPONSE_ID: Final = "gen_ai.response.id"
"""
The unique identifier for the completion.
"""

GEN_AI_RESPONSE_MODEL: Final = "gen_ai.response.model"
"""
The name of the model that generated the response.
"""

GEN_AI_SYSTEM: Final = "gen_ai.system"
"""
The Generative AI product as identified by the client or server instrumentation.
Note: The `gen_ai.system` describes a family of GenAI models with specific model identified
by `gen_ai.request.model` and `gen_ai.response.model` attributes.

The actual GenAI product may differ from the one identified by the client.
For example, when using OpenAI client libraries to communicate with Mistral, the `gen_ai.system`
is set to `openai` based on the instrumentation's best knowledge.

For custom model, a custom friendly name SHOULD be used.
If none of these options apply, the `gen_ai.system` SHOULD be set to `_OTHER`.
"""

GEN_AI_TOKEN_TYPE: Final = "gen_ai.token.type"
"""
The type of token being counted.
"""

GEN_AI_USAGE_COMPLETION_TOKENS: Final = "gen_ai.usage.completion_tokens"
"""
Deprecated: Replaced by `gen_ai.usage.output_tokens` attribute.
"""

GEN_AI_USAGE_INPUT_TOKENS: Final = "gen_ai.usage.input_tokens"
"""
The number of tokens used in the GenAI input (prompt).
"""

GEN_AI_USAGE_OUTPUT_TOKENS: Final = "gen_ai.usage.output_tokens"
"""
The number of tokens used in the GenAI response (completion).
"""

GEN_AI_USAGE_PROMPT_TOKENS: Final = "gen_ai.usage.prompt_tokens"
"""
Deprecated: Replaced by `gen_ai.usage.input_tokens` attribute.
"""


class GenAiOperationNameValues(Enum):
    CHAT = "chat"
    """Chat completion operation such as [OpenAI Chat API](https://platform.openai.com/docs/api-reference/chat)."""
    TEXT_COMPLETION = "text_completion"
    """Text completions operation such as [OpenAI Completions API (Legacy)](https://platform.openai.com/docs/api-reference/completions)."""


class GenAiSystemValues(Enum):
    OPENAI = "openai"
    """OpenAI."""
    VERTEX_AI = "vertex_ai"
    """Vertex AI."""
    ANTHROPIC = "anthropic"
    """Anthropic."""
    COHERE = "cohere"
    """Cohere."""


class GenAiTokenTypeValues(Enum):
    INPUT = "input"
    """Input tokens (prompt, input, etc.)."""
    COMPLETION = "output"
    """Output tokens (completion, response, etc.)."""
