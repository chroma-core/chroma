---
id: package-search-mcp
name: Package Search MCP
---

# Package Search MCP Server

The Package Search MCP Server is an [MCP](https://modelcontextprotocol.io/docs/getting-started/intro) server designed to add ground truth context about code packages to AI agents. Our research demonstrates that by exposing the source code of a project's dependencies to a model, we improve its performance on coding tasks and reduce its potential for hallucination. Chroma's Package Search MCP server achieves this by exposing tools to allow the model to retrieve necessary context:

| Tool Name                  | Usage                                                                                                                |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `package_search_grep`      | Use regex pattern matching to retrieve relevant lines from source code                                               |
| `package_search_hybrid`    | Use semantic search with optional regex filtering to explore source code without existing knowledge of its structure |
| `package_search_read_file` | Reads specific lines from a single file in the code package                                                          |

## Getting Started

{% Banner type="note" %}

To guarantee that your model uses package search when desired, add `use package search` to either the system prompt (to use the MCP server whenever applicable) or to each task prompt (to use it only when you instruct the model to do so).

{% /Banner %}

{% ComboboxSteps defaultValue="anthropic-sdk" itemType="environment" %}

{% Step %}
Visit Chroma's [Package Search](http://trychroma.com/package-search) page.
{% /Step %}

{% Step %}
Click "Get API Key" to create or log into your Chroma account and issue an API key for Package Search.
{% /Step %}

{% Step %}
After issuing your API key, click the "Other" tab and copy your API key.
{% /Step %}

{% ComboboxEntry value="anthropic-sdk" label="Anthropic SDK" %}
{% Step %}
Connect to the Chroma MCP server to search code packages. In this example, we search for how the Fast Fourier Transform algorithm is implemented in the `numpy` package from PyPI.

{% TabbedCodeBlock %}
{% Tab label="python" %}

```python
import anthropic

client = anthropic.Anthropic(
    api_key="<YOUR_ANTHROPIC_API_KEY>"
)

response = client.beta.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1000,
    messages=[
        {
            "role": "user",
            "content": "Explain how numpy implements its FFT. Use package search.",
        }
    ],
    mcp_servers=[
        {
            "type": "url",
            "url": "https://mcp.trychroma.com/package-search/v1",
            "name": "package-search",
            "authorization_token": "<YOUR_CHROMA_API_KEY>",
        }
    ],
    betas=["mcp-client-2025-04-04"],
)

print(response)
```

{% /Tab %}
{% Tab label="Go" %}

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
)

func main() {
	client := anthropic.NewClient(
		option.WithAPIKey("<YOUR_ANTHROPIC_API_KEY>"),
		option.WithHeader("anthropic-beta", anthropic.AnthropicBetaMCPClient2025_04_04),
	)

	content := "Explain how numpy implements its FFT. Use package search."
	fmt.Println("[user]:", content)

	messages := []anthropic.BetaMessageParam{
		anthropic.NewBetaUserMessage(
			anthropic.NewBetaTextBlock(content),
		),
	}

	mcpServers := []anthropic.BetaRequestMCPServerURLDefinitionParam{
		{
			URL:                "https://mcp.trychroma.com/package-search/v1",
			Name:               "package-search",
			AuthorizationToken: param.NewOpt("<YOUR_CHROMA_API_KEY>"),
			ToolConfiguration: anthropic.BetaRequestMCPServerToolConfigurationParam{
				Enabled:      anthropic.Bool(true),
			},
		},
	}

	message, err := client.Beta.Messages.New(
		context.TODO(),
		anthropic.BetaMessageNewParams{
			MaxTokens:  1024,
			Messages:   messages,
			Model:      anthropic.ModelClaudeSonnet4_20250514,
			MCPServers: mcpServers,
		},
	)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}

	for _, block := range message.Content {
		textBlock := block.AsText()
		fmt.Println("[assistant]:", textBlock.Text)
	}
}
```

{% /Tab %}
{% /TabbedCodeBlock %}

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="openai-sdk" label="OpenAI SDK" %}
{% Step %}
Connect to the Chroma MCP server to search code packages. In this example, we search for class definitions in the `numpy` package from PyPI.

```python
from openai import OpenAI

client = OpenAI(
    api_key="<YOUR_OPENAI_API_KEY>"
)

resp = client.responses.create(
    model="gpt-5-chat-latest",
    input="Explain how numpy implements its FFT. Use package search.",
    tools=[
        {
            "type": "mcp",
            "server_label": "package-search",
            "server_url": "https://mcp.trychroma.com/package-search/v1",
            "headers": {
                "x-chroma-token": "<YOUR_CHROMA_API_KEY>"
            },
            "require_approval": "never",
        }
    ],
)

print(resp)
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="google-gemini-sdk" label="Google Gemini SDK" %}

{% Step %}
Get a Gemini API key in [Google's AI Studio](https://aistudio.google.com/app/apikey)
{% /Step %}

{% Step %}
Connect the Chroma MCP server with Gemini to enable AI-powered code searches. In this example, we ask Gemini to explain how the Fast Fourier Transform algorithm is implemented in `numpy`, using the Chroma MCP tools to search and analyze the code.

```python
import asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from google import genai

client = genai.Client(api_key="<YOUR_GEMINI_API_KEY>")

async def run():
    async with streamablehttp_client(
        "https://mcp.trychroma.com/package-search/v1",
        headers={"x-chroma-token": "<YOUR_CHROMA_API_KEY>"},
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            try:
                prompt = f"Explain how numpy implements its FFT. Use package search."
                response = await client.aio.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config=genai.types.GenerateContentConfig(
                        temperature=0,
                        tools=[session],
                    ),
                )
                try:
                    if response.text:
                        print("--- Generated Text ---")
                        print(response.text)
                    else:
                        print("Model did not return text.")
                        print(f"Finish Reason: {response.candidates[0].finish_reason.name}")
                except ValueError:
                    print("Could not access response.text.")
            except Exception as e:
                print(f"An error occurred: {e}")

asyncio.run(run())
```

{% /Step %}

{% /ComboboxEntry %}

{% ComboboxEntry value="claude-code" label="Claude Code" %}
{% Step %}
Add the Chroma MCP server to Claude Code with your Chroma API key:

```terminal
claude mcp add --transport http package-search https://mcp.trychroma.com/package-search/v1 --header "x-chroma-token: <YOUR_CHROMA_API_KEY>"
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="codex" label="Codex" %}
{% Step %}
Add the following to your `~/.codex/config.toml` file with your Chroma Cloud API key:

```TOML
[mcp_servers.package-search]
command = "npx"
args = ["mcp-remote", "https://mcp.trychroma.com/package-search/v1", "--header", "x-chroma-token: ${X_CHROMA_TOKEN}"]
env = { "X_CHROMA_TOKEN" = "<YOUR_CHROMA_API_KEY>" }
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="cursor" label="Cursor" %}
{% Step %}
In Cursor's settings, search for "MCP" and add the following configuration with your Chroma Cloud API key:

```JSON
{
  "mcpServers": {
    "package-search": {
      "transport": "streamable_http",
      "url": "https://mcp.trychroma.com/package-search/v1",
      "headers": {
        "x-chroma-token": "<YOUR_CHROMA_API_KEY>"
      }
    }
  }
}
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="windsurf" label="Windsurf" %}
{% Step %}
In Windsurf's settings, search for "MCP" and add the following configuration with your Chroma Cloud API key:

```JSON
{
  "mcpServers": {
    "package-search": {
      "serverUrl": "https://mcp.trychroma.com/package-search/v1",
      "headers": {
        "x-chroma-token": "<YOUR_CHROMA_API_KEY>"
      }
    }
  }
}
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="claude-desktop" label="Claude Desktop" %}
{% Step %}
Add the following to your `~/Library/Application Support/Claude/claude_desktop_config.json`:

```JSON
{
    "mcpServers": {
      "package-search": {
        "command": "npx",
        "args": ["mcp-remote", "https://mcp.trychroma.com/package-search/v1", "--header", "x-chroma-token: ${X_CHROMA_TOKEN}"],
        "env": {
          "X_CHROMA_TOKEN": "<YOUR_CHROMA_API_KEY>"
        }
      }
    }
}
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="warp" label="Warp" %}
{% Step %}
Add the following to your Warp MCP config. Make sure to click "Start" on the server after adding.

```JSON
{
    "package-search": {
      "command": "npx",
      "args": ["mcp-remote", "https://mcp.trychroma.com/package-search/v1", "--header", "x-chroma-token: ${X_CHROMA_TOKEN}"],
      "env": {
        "X_CHROMA_TOKEN": "<YOUR_CHROMA_API_KEY>"
      }
    }
}
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="open-code" label="Open Code" %}
{% Step %}
Add the following to your `~/.config/opencode/opencode.json` file with your Chroma Cloud API key:

```JSON
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "code-packages": {
      "type": "remote",
      "url": "https://mcp.trychroma.com/package-search/v1",
      "enabled": true,
      "headers": {
        "x-chroma-token": "<YOUR_CHROMA_API_KEY>"
      }
    }
  }
}
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="ollama" label="Ollama" %}

{% Step %}
Install the `ollmcp` package:
{% PythonInstallation packages="ollmcp" / %}
{% /Step %}

{% Step %}
Create an `mcp_config.json` file with the following content and your Chroma Cloud API key:

```JSON
{
	"mcpServers": {
		"code-packages": {
			"type": "streamable_http",
			"url": "https://mcp.trychroma.com/package-search/v1",
			"headers": {
				"x-chroma-token": "<YOUR_CHROMA_API_KEY>"
			},
			"disabled": false
		}
	}
}
```

{% /Step %}

{% Step %}
Start an Ollama MCP session with the path to your `mcp_config.json` file and model of choice:

```terminal
ollmcp --servers-json <path/to/mcp_config.json> --model <model>
```

{% /Step %}

{% /ComboboxEntry %}

{% ComboboxEntry value="mcp-sdk" label="MCP SDK" %}
{% Step %}
Connect to the Chroma MCP server to search code packages. In this example, we search for the Fast Fourier Transform function in the `numpy` package from PyPI using the `package_search_grep` tool.

```python
import asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

async def main():
    async with streamablehttp_client(
        "https://mcp.trychroma.com/package-search/v1",
        headers={"x-chroma-token": "<YOUR_CHROMA_API_KEY>"},
    ) as (
        read_stream,
        write_stream,
        _,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools = await session.list_tools()
            result = await session.call_tool(
                name="package_search_grep",
                arguments={
                    "package_name": "numpy",
                    "registry_name": "py_pi",
                    "pattern": "\bdef fft\b",
                },
            )
            print(f"Got result: {result}")
            print(f"Available tools: {[tool.name for tool in tools.tools]}")

asyncio.run(main())
```

{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="roo-code" label="Roo Code" %}
{% Step %}
Add this to your Roo Code MCP server configuration:

```JSON
{
  "mcpServers": {
    "code-collections": {
      "type": "streamable-http",
      "url": "https://mcp.trychroma.com/package-search/v1",
      "headers": {
        "x-chroma-token": "<YOUR_CHROMA_API_KEY>"
      }
    }
  }
}
```

{% /Step %}
{% /ComboboxEntry %}

{% /ComboboxSteps %}
