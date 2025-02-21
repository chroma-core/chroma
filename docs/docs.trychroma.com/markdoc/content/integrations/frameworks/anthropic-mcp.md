---
id: anthropic-mcp
name: Anthropic MCP
---

# Anthropic MCP

The Model Context Protocol (MCP) is an open protocol that standardizes how applications provide context to LLMs. Think of MCP like a USB-C port for AI applications - it provides a standardized way to connect AI models to different data sources and tools.

The Chroma MCP server implements this protocol to allow LLMs like Claude to seamlessly interact with Chroma's vector database capabilities.

## Features

- Create and manage Chroma collections
- Add, update, and query documents
- Perform vector and keyword searches
- Manage embeddings and metadata
- Execute batch operations
- Handle persistent storage

## Setup

### 1. Configure MCP Server

Add the following to your `claude_desktop_config.json` to enable the Chroma MCP server:

```json
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
```

## Usage

The MCP server exposes Chroma's core functionality through a standardized interface:

- Collection Management: Create, delete, and modify collections
- Document Operations: Add, update, delete documents
- Search Capabilities: Vector similarity search, keyword search, filtering
- Metadata Management: Add and query metadata
- Embedding Generation: Automatic embedding of text content
- Batch Processing: Efficient handling of multiple operations

## Example: Adding Memory to Claude Projects

Here's an example of how to use the Chroma MCP server to give Claude persistent memory of conversations.

### 1. Enable Memory Context

Add this text to your project context:

```
You have access to Chroma tools.
If the user references previous chats or memory, check chroma for similar conversations.
If you find similar conversations, use that where possible.
```

### 2. Store Conversations

To store a conversation, tell Claude:

```
Chunk our entire conversation into small embeddable text chunks, no longer than a couple lines each.
Then, add it to the collection for this project.
```

Claude will automatically:
- Break conversations into appropriate chunks
- Generate unique IDs
- Add relevant metadata and labels
- Store in Chroma for future reference

### 3. Access Previous Conversations

When users reference past discussions, Claude can:
- Search for semantically similar conversation chunks
- Use full text search when appropriate
- Filter results using metadata
- Incorporate relevant past context into responses

Example query:
```
Can we continue our conversation about cuda threading?
```

## Resources

- [Chroma MCP Server Documentation](https://github.com/chroma-core/chroma-mcp)
- [Model Context Protocol Specification](https://github.com/anthropics/anthropic-mcp)
- [Understanding MCP](https://docs.anthropic.com/claude/docs/model-context-protocol)
