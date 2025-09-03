# Code Collections MCP Server

The Code Collections MCP Server is a [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports#streamable-http) MCP server implemented according to the `2025-06-18` MCP specification. It requires a custom authorization header, `x-chroma-token`, to authenticate incoming requests. Since custom authorization headers were introduced in the latest version of the MCP specification, you'll need to use [mcp-remote](https://github.com/geelen/mcp-remote) if your environment runs an older version of the spec. Using `mcp-remote` requires no additional steps beyond ensuring the `npx` executable is available on your system.

You can continue using your existing system for connecting to MCP servers (remote or local) to access Chroma's Code Collections server. Simply provide these details in your MCP configuration parameters:

| Parameter | Value                                    |
|-----------|------------------------------------------|
| URL       | https://mcp.trychroma.com/package-search |
| Headers   | `x-chroma-token: <YOUR_CHROMA_API_KEY>`  |
| Type      | `streamble-http`                         |

{% Banner type="note" %}

Different platforms have different conventions for naming transports. We recommend referring to your platform’s documentation for the exact specification, but common names include `http`, `streamble-http`, or `streamble_http`.

{% /Banner %}

## Setup

{% ComboboxSteps %}

{% Step %}
Sign up
{% /Step %}

{% /ComboboxSteps %}