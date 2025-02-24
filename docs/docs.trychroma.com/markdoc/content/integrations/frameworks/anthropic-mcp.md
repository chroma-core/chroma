---
id: anthropic-mcp
name: Anthropic MCP
---

# Anthropic MCP Integration

## What is MCP?

The Model Context Protocol (MCP) is an open protocol that standardizes how AI applications communicate with data sources and tools. Think of MCP like a USB-C port for AI applications - it provides a universal way to connect AI models like Claude to different services and data sources.

MCP follows a client-server architecture:
- **MCP Hosts**: Applications like Claude Desktop that want to access data through MCP
- **MCP Clients**: Protocol clients that maintain connections with servers
- **MCP Servers**: Lightweight programs that expose specific capabilities (like Chroma's vector database)
- **Data Sources**: Your local or remote data that MCP servers can securely access

## What is the Chroma MCP Server?

The Chroma MCP server allows Claude to directly interact with Chroma's vector database capabilities through this standardized protocol. This enables powerful features like:

- Persistent memory across conversations
- Semantic search through previous chats
- Document management and retrieval
- Vector and keyword search capabilities
- Metadata management and filtering

## Prerequisites

Before setting up the Chroma MCP server, ensure you have:

1. Claude Desktop installed (Windows or macOS)
2. Python 3.10+ installed
3. `uvx` installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

## Setup Guide

### 1. Configure MCP Server

1. Open Claude Desktop
2. Click on the Claude menu and select "Settings..."
3. Click on "Developer" in the left sidebar
4. Click "Edit Config" to open your configuration file

Add the following configuration:

```json
{
  "mcpServers": {
    "chroma": {
      "command": "uvx",
      "args": [
        "chroma-mcp",
        "--client-type",
        "persistent",
        "--data-dir",
        "/path/to/your/data/directory"
      ]
    }
  }
}
```

Replace `/path/to/your/data/directory` with where you want Chroma to store its data, for example:
- macOS: `/Users/username/Documents/chroma-data`
- Windows: `C:\\Users\\username\\Documents\\chroma-data`

### 2. Restart and Verify

1. Restart Claude Desktop completely
2. Look for the hammer 🔨 icon in the bottom right of your chat input
3. Click it to see available Chroma tools

If you don't see the tools, check the logs at:
- macOS: `~/Library/Logs/Claude/mcp*.log`
- Windows: `%APPDATA%\Claude\logs\mcp*.log`

## Client Types

The Chroma MCP server supports multiple client types to suit different needs:

### 1. Ephemeral Client
```json
{
  "mcpServers": {
    "chroma": {
      "command": "uvx",
      "args": [
        "chroma-mcp",
      ]
    }
  }
}
```
- Stores data in memory only
- Data is cleared when the server restarts
- Useful for temporary sessions or testing

### 2. Persistent Client (Default)
```json
{
  "mcpServers": {
    "chroma": {
      "command": "uvx",
      "args": [
        "chroma-mcp",
        "--client-type",
        "persistent",
        "--data-dir",
        "/path/to/your/data/directory"
      ]
    }
  }
}
```
- Stores data persistently on your local machine
- Data survives between restarts
- Best for personal use and long-term memory


### 3. Self-Hosted Client
```json
{
  "mcpServers": {
    "chroma": {
      "command": "uvx",
      "args": [
        "chroma-mcp",
        "--client-type",
        "http",
        "--host",
        "http://localhost:8000"
      ]
    }
  }
}
```
- Connects to your own Chroma server
- Full control over data and infrastructure
- Suitable for team environments

### 4. Cloud Client
```json
{
  "mcpServers": {
    "chroma": {
      "command": "uvx",
      "args": [
        "chroma-mcp",
        "--client-type",
        "http",
        "--host",
        "https://your-chroma-cloud.example.com",
        "--token",
        "your-api-token"
      ]
    }
  }
}
```
- Connects to Chroma Cloud or other hosted instances
- Scalable and managed infrastructure
- Best for production deployments

## Using Chroma with Claude

### Basic Memory Storage

To store the current conversation:
```
Claude, please chunk our conversation into small chunks and store it in Chroma for future reference.
```

Claude will automatically:
- Break the conversation into appropriate chunks
- Generate embeddings and unique IDs
- Add metadata (like timestamps and topics)
- Store everything in your Chroma collection

### Accessing Previous Conversations

To recall past discussions:
```
Claude, what did we discuss previously about vector databases?
```

Claude will:
1. Search Chroma for semantically similar conversation chunks
2. Filter results based on relevance
3. Incorporate previous context into its response

### Advanced Features

The Chroma MCP server supports:

- **Collection Management**: Create and organize separate collections for different projects
- **Document Operations**: Add, update, or delete documents
- **Search Capabilities**:
  - Vector similarity search
  - Keyword-based search
  - Metadata filtering
- **Batch Processing**: Efficient handling of multiple operations

## Troubleshooting

If you encounter issues:

1. Verify your configuration file syntax
2. Ensure all paths are absolute and valid
3. Try using full paths for `uvx` with `which uvx` and using that path in the config
4. Check the Claude logs (paths listed above)

## Resources

- [Model Context Protocol Documentation](https://modelcontextprotocol.io/introduction)
- [Chroma MCP Server Documentation](https://github.com/chroma-core/chroma-mcp)
- [Claude Desktop Guide](https://docs.anthropic.com/claude/docs/claude-desktop)
