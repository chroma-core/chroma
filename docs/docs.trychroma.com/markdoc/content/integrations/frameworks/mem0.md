---
id: mem0
name: Mem0
---

# Mem0

Mem0 is an AI memory layer that transforms stateless AI agents into stateful systems with persistent, intelligent memory across interactions. It enables AI applications to remember, learn, and evolve by providing different types of memory including working memory, factual memory, episodic memory, and semantic memory.

## Installation

```bash
pip install mem0ai chromadb
```

## Configuration

Mem0 can be configured to use Chroma as its vector database backend. Here are the available configuration options:

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| `collection_name` | Name of the Chroma collection | `mem0` |
| `client` | Custom Chroma client | `None` |
| `path` | Path for the Chroma database | `db` |
| `host` | Chroma server host | `None` |
| `port` | Chroma server port | `None` |

## Basic Usage

### Using Mem0 with Local Chroma

```python
import os
from mem0 import Memory

# Set your OpenAI API key
os.environ["OPENAI_API_KEY"] = "sk-your-openai-key"

# Configure Mem0 with Chroma
config = {
    "vector_store": {
        "provider": "chroma",
        "config": {
            "collection_name": "my_memories",
            "path": "chroma_db",
        }
    }
}

# Initialize memory
memory = Memory.from_config(config)

# Add memories from conversation
messages = [
    {"role": "user", "content": "I'm planning to watch a movie tonight. Any recommendations?"},
    {"role": "assistant", "content": "How about thriller movies? They can be quite engaging."},
    {"role": "user", "content": "I'm not a big fan of thriller movies but I love sci-fi movies."},
    {"role": "assistant", "content": "Got it! I'll avoid thriller recommendations and suggest sci-fi movies in the future."}
]

memory.add(messages, user_id="alice", metadata={"category": "movies"})

# Search memories
relevant_memories = memory.search("movie preferences", user_id="alice")
print(relevant_memories)
```

## Use Cases

- **Personalized AI Assistants**: Remember user preferences and context across sessions
- **Customer Support**: Maintain conversation history and customer preferences
- **Educational Systems**: Track learning progress and adapt to student needs
- **Research Tools**: Build knowledge bases from interactions
- **Multi-session Applications**: Provide continuity across conversation sessions

## Resources

- [Mem0 Documentation](https://docs.mem0.ai/)
- [Mem0 Chroma Integration](https://docs.mem0.ai/components/vectordbs/dbs/chroma)
- [Mem0 GitHub Repository](https://github.com/mem0ai/mem0)