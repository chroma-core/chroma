# Ephemeral Client

In Python, you can run a Chroma server in-memory and connect to it with the ephemeral client:

```python
import chromadb

client = chromadb.Client()
```

This is a great tool for experimenting with different embedding functions and retrieval techniques in a Python notebook, for example. 