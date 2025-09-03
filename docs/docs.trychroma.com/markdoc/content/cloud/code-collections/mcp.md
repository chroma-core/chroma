# Code Collections MCP Server

The Code Collections MCP Server is a [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports#streamable-http) MCP server implemented according to the `2025-06-18` MCP specification. It requires a custom authorization header, `x-chroma-token`, to authenticate incoming requests. Since custom authorization headers were introduced in the latest version of the MCP specification, you'll need to use [mcp-remote](https://github.com/geelen/mcp-remote) if your environment runs an older version of the spec. Using `mcp-remote` requires no additional steps beyond ensuring the `npx` executable is available on your system.

You can continue using your existing system for connecting to MCP servers (remote or local) to access Chroma's Code Collections server. Simply provide these details in your MCP configuration parameters:

| Parameter | Value                                      |
|-----------|--------------------------------------------|
| URL       | `https://mcp.trychroma.com/package-search` |
| Headers   | `x-chroma-token: <YOUR_CHROMA_API_KEY>`    |
| Type      | `streamble-http`                           |

{% Banner type="note" %}

Different platforms have different conventions for naming transports. We recommend referring to your platform’s documentation for the exact specification, but common names include `http`, `streamble-http`, or `streamble_http`.

{% /Banner %}

## Setup

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
claude mcp add --transport http chroma-code-mcp https://mcp.trychroma.com/mcp --header "x-chroma-token: <YOUR_CHROMA_API_KEY>" 
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
        "https://mcp.trychroma.com/mcp",
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
Add the following to your `~/.config/opencode/opencode.json` file:
```JSON
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "code-packages": {
      "type": "remote",
      "url": "https://mcp.trychroma.com/mcp",
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
Install the `ollmcp` package
{% /Step %}
{% /ComboboxEntry %}

{% /ComboboxSteps %}