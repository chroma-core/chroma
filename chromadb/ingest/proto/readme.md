# Protocol Buffer Generation

The Python files in this directory are auto-generated: do not edit them directly.

If you edit the Protocol Buffer definitions (`*.proto` files), you can regenerate the python stubs using the following commands, from the project root:

```
protoc --python_out=. --mypy_out=. chromadb/ingest/proto/chroma.proto
```