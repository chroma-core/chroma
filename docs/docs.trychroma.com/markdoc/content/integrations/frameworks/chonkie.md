---
id: chonkie
name: Chonkie
---

# Chonkie
[Chonkie](https://docs.chonkie.ai) is an open source library for data ingestion. 
Chonkie provides advanced chunkers for cleanly splitting your data, and embedding handlers to embed the resulting chunks.

When building a RAG system, you can have your data chunked and embeded by Chonkie, and then pass it on to the Chroma handshake for storage and retreival from your Chroma instance.

{% Banner type="tip" %}
For more information on how to use Chonkie, see the [Chonkie Docs](https://docs.chonkie.ai)
{% /Banner %}


# Chonkie Chroma Handshake

Chonkie provides a Chroma handshake that can be used to embed and insert data into a Chroma collection.

## Prerequisites

Install Chonkie with Chroma dependencies:
```bash
pip install "chonkie[chroma]"
```

## Usage

### Chunking With Chonkie
```python
from chonkie import SemanticChunker

text = "Chonkie and Chroma - Best Friends For Life!"
chunker = SemanticChunker() # See docs.chonkie.ai for more information on chunkers

# Chunk your data
chunks = chunker(text)
```

### Initialize Chroma Handshake
```python
from chonkie import ChromaHandshake

# Initialize with default settings (in-memory ChromaDB)
handshake = ChromaHandshake()

# Or specify a persistent storage path
handshake = ChromaHandshake(path="./chroma_db")

# Or use an existing Chroma client
import chromadb
client = chromadb.Client()
handshake = ChromaHandshake(client=client, collection_name="my_collection")

# Feature: Select embedding model to use
handshake = ChromaHandshake(embedding_model="text-embedding-ada-002")
```

### Writing Chunks to Chroma
```python
from chonkie import ChromaHandshake, SemanticChunker

handshake = ChromaHandshake() # Initializes a new Chroma client

text = "Chonkie and Chroma - Best Friends For Life!"
chunker = SemanticChunker()
chunks = chunker(text)

handshake.write(chunks)
```

## Resources

- [Chonkie Documentation](https://docs.chonkie.ai)
- [Chonkie Chroma Handshake Documentation](https://docs.chonkie.ai/python-sdk/handshakes/chroma-handshake)
- [Chonkie Discord](https://discord.gg/6V5pqvqsCY)

