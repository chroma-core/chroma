# Package Search MCP Server

The Package Search MCP Server is a [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports#streamable-http) MCP server implemented according to the `2025-06-18` MCP specification. It requires a custom authorization header, `x-chroma-token`, to authenticate incoming requests. Since custom authorization headers were introduced in the latest version of the MCP specification, you'll need to use [mcp-remote](https://github.com/geelen/mcp-remote) if your environment runs an older version of the spec. Using `mcp-remote` requires no additional steps beyond ensuring the `npx` executable is available on your system.

You can continue using your existing system for connecting to MCP servers (remote or local) to access Chroma's Package Search server. Simply provide these details in your MCP configuration parameters:

| Parameter | Value                                      |
|-----------|--------------------------------------------|
| URL       | `https://mcp.trychroma.com/package-search/v1` |
| Headers   | `x-chroma-token: <YOUR_CHROMA_API_KEY>`    |
| Type      | `streamable-http`                           |

{% Banner type="note" %}

Different platforms have different conventions for naming transports. We recommend referring to your platform’s documentation for the exact specification, but common names include `http`, `streamble-http`, or `streamble_http`.

{% /Banner %}

## Getting started

Visit the [Package Search installation page](https://trychroma.com/package-search) for quick setup in most clients.

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
Connect to the Chroma MCP server to search code packages. In this example, we search for class definitions in the `colorlog` package from PyPI using the `code_package_search` tool.
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
                name="code_package_search",
                arguments={
                    "package_name": "colorlog",
                    "registry_name": "py_pi",
                    "grep": "\bclass\b",
                },
            )
            print(f"Got result: {result}")
            print(f"Available tools: {[tool.name for tool in tools.tools]}")


if __name__ == "__main__":
    asyncio.run(main())
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
ollmcp --servers-json <path/to/mcp_config.json> --model qwen2.5
```
{% /Step %}

{% /ComboboxEntry %}

{% ComboboxEntry value="google-gemini" label="Google Gemini" %}

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
[mcp_servers.server-name]
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

## Direct Queries

To quickly test Chroma's Package Search, simply ask your model a question about a specific package. For instance, if you're wondering about the implementation of `join_set` in Rust's `tokio` crate, just ask: "How is join_set implemented in `tokio`?" Unless you specify a particular version, the MCP server will default to the latest version of `tokio` available in Chroma's index.

## Automatic Queries

Developers don't always think to directly ask about implementation details in dependencies. It's more valuable when your model knows when to use the Package Search MCP server automatically when encountering uncertainty about third-party libraries. We've carefully crafted our tool descriptions to make this happen naturally, so you don't need to take any additional steps beyond following the Setup Guide above. If your model isn't using the Package Search tools when you think it should, please share this feedback with us through your shared Slack channel or via support@trychroma.com.

## Known Issues

1. **Over-querying:** Models often fall into loops of repeatedly using grep/hybrid search functions, gathering more information than needed. This issue will be addressed in a future prompt upgrade. Enabling the "thinking" feature provides another solution, giving the model more time to reason before making tool calls.
2. **Lack of parameter utilization:** Models frequently fail to use available non-required arguments (such as a, b, c in grep) that could help them find necessary context more efficiently. Instead, they create unnecessarily complex regex patterns.
