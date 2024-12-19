# The Python HTTP-Only Client

If you are running Chroma in client-server mode, where you run a Chroma server and client on separate machines, you may not need the full Chroma package where you run your client. Instead, you can use the lightweight client-only library.
In this case, you can install the `chromadb-client` package. This package is a lightweight HTTP client for the server with a minimal dependency footprint.

On your server, install chroma with

```terminal
pip install chromadb
```

And run a Chroma server:

```terminal
chroma run --path [path/to/persist/data]
```

Then, on your client side, install the HTTP-only client: 

```terminal
pip install chromadb-client
```

```python
import chromadb
# Example setup of the client to connect to your chroma server
client = chromadb.HttpClient(host='localhost', port=8000)

# Or for async usage:
async def main():
    client = await chromadb.AsyncHttpClient(host='localhost', port=8000)
```

Note that the `chromadb-client` package is a subset of the full Chroma library and does not include all the dependencies. If you want to use the full Chroma library, you can install the `chromadb` package instead.
Most importantly, there is no default embedding function. If you add() documents without embeddings, you must have manually specified an embedding function and installed the dependencies for it.
