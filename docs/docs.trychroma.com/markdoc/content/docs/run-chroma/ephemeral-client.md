# Ephemeral Client

In Python, you can run a Chroma server in-memory and connect to it with the ephemeral client:

```python
import chromadb

client = chromadb.Client()
```

The `Client()` method starts a Chroma server in-memory and also returns a client with which you can connect to it.

This is a great tool for experimenting with different embedding functions and retrieval techniques in a Python notebook, for example. If you don't need data persistence, the ephemeral client is a good choice for getting up and running with Chroma.