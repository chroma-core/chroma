# Package Search MCP Server

The Package Search MCP Server is an [MCP](https://modelcontextprotocol.io/docs/getting-started/intro) server designed to add ground truth context about code packages to AI agents. Our research demonstrates that by exposing dependency source code, model performance is enhanced across all tasks and reduces the potential for hallucinations. The server does this by exposing tools to allow the model to retrieve necessary context:

| Tool Name | Usage                                      |
|-----------|--------------------------------------------|
| `package_search_grep`       | Use regex pattern matching to retrieve relevant lines from dependency |
| `package_search_hybrid`   | Use semantic search and/or regex to execute semantically meaningful queries across code     |
| `package_search_read_file`      | Reads specific lines from a single file in the code package   |

## Getting started

Visit the [Package Search installation page](https://trychroma.com/package-search) for quick setup in most clients.

{% Banner type="note" %}

To guarantee the model uses package search when desired, add `use package search` to the prompt.

{% /Banner %}

## Configuration

{% ComboboxSteps defaultValue="claude-code" %}

{% Step %}
[Sign up](https://trychroma.com/signup) for a Chroma Cloud account.
{% /Step %}

{% Step %}
On the dashboard's homepage, click on the {% ImageHoverText src="code-collections-settings.png" %}Settings button{% /ImageHoverText %}.
{% /Step %}

{% Step %}
In the Setting's menu, click on the {% ImageHoverText src="code-collections-api-keys.png" %}API Keys tab{% /ImageHoverText %}, and then on the {% ImageHoverText src="code-collections-api-keys.png" %}Create button{% /ImageHoverText %} to generate a key. Copy your API key, as you will need it to connect to the MCP server.
{% /Step %}

{% ComboboxEntry value="claude-code" label="Claude Code" %}
{% Step %}
Add the Chroma MCP server to Claude Code with your Chroma API key
```terminal
claude mcp add --transport http package-search https://mcp.trychroma.com/package-search/v1 --header "x-chroma-token: <YOUR_CHROMA_API_KEY>"
```
{% /Step %}
{% /ComboboxEntry %}

{% ComboboxEntry value="mcp-sdk" label="MCP SDK" %}
{% Step %}
Connect to the Chroma MCP server to search code packages. In this example, we search for class definitions in the `colorlog` package from PyPI using the `package_search_grep` tool.
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
                    "package_name": "colorlog",
                    "registry_name": "py_pi",
                    "pattern": "\bclass\b",
                },
            )
            print(f"Got result: {result}")
            print(f"Available tools: {[tool.name for tool in tools.tools]}")


if __name__ == "__main__":
    asyncio.run(main())
```
{% /Step %}
{% /ComboboxEntry %}


{% ComboboxEntry value="openai-sdk" label="OpenAI SDK" %}
{% Step %}
Connect to the Chroma MCP server to search code packages. In this example, we search for class definitions in the `colorlog` package from PyPI.
```python
from openai import OpenAI

client = OpenAI(
    api_key="<YOUR_OPENAI_API_KEY>"
)

resp = client.responses.create(
    model="gpt-5-chat-latest",
    input="Explain how colorlog implements testing in python",
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

{% ComboboxEntry value="anthropic-sdk" label="Anthropic SDK" %}
{% Step %}
Connect to the Chroma MCP server to search code packages. In this example, we search for class definitions in the `colorlog` package from PyPI.

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
            "content": "Explain how colorlog implements testing in python",
        }
    ],
    mcp_servers=[
        {
            "type": "url",
            "url": "https://mcp.trychroma.com/package-search/v1",
            "name": "code-collections",
            "authorization_token": "<YOUR_CHROMA_API_KEY>",
        }
    ],
    betas=["mcp-client-2025-04-04"],
)

print(response)
```
{% /Tab %}
{% Tab label="Go" %}
```Go
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
	// Init client (supply API key via env or here explicitly)
	client := anthropic.NewClient(
		option.WithAPIKey("<YOUR_ANTHROPIC_API_KEY>"),
		option.WithHeader("anthropic-beta", anthropic.AnthropicBetaMCPClient2025_04_04),
	)

	// User message
	content := "Explain how colorlog implements testing in python"
	fmt.Println("[user]:", content)

	// Build messages
	messages := []anthropic.BetaMessageParam{
		anthropic.NewBetaUserMessage(
			anthropic.NewBetaTextBlock(content),
		),
	}

	// MCP server configuration
	mcpServers := []anthropic.BetaRequestMCPServerURLDefinitionParam{
		{
			URL:                "https://mcp.trychroma.com/package-search/v1",
			Name:               "code-collections",
			AuthorizationToken: param.NewOpt("<YOUR_CHROMA_API_KEY>"),
			ToolConfiguration: anthropic.BetaRequestMCPServerToolConfigurationParam{
				Enabled:      anthropic.Bool(true),
			},
		},
	}

	// Make a single non-streaming request
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

	// Print result content
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
ollmcp --servers-json <path/to/mcp_config.json> --model qwen2.5
```
{% /Step %}

{% /ComboboxEntry %}

{% ComboboxEntry value="google-gemini-sdk" label="Google Gemini SDK" %}

{% Step %}
Get a Gemini API key in [Google's AI Studio](https://aistudio.google.com/app/apikey)
{% /Step %}

{% Step %}
Connect the Chroma MCP server with Gemini to enable AI-powered code searches. In this example, we ask Gemini to find logging levels in Uber's Zap Go module, with Gemini using the Chroma MCP tools to search and analyze the code.

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

            prompt = f"what logging levels are available in uber's zap go module?"

            await session.initialize()

            try:
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
                        print(
                            f"Finish Reason: {response.candidates[0].finish_reason.name}"
                        )

                except ValueError:
                    print(
                        "Could not access response.text. The response may have been blocked or contain non-text parts."
                    )

            except Exception as e:
                print(f"An error occurred: {e}")

asyncio.run(run())
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
env = { "X_CHROMA_TOKEN" = "<your-key>" }
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

{% /ComboboxSteps %}
