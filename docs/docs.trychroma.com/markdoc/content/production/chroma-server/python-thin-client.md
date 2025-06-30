# Chroma's Thin-Client


If you are running Chroma in client-server mode in a Python or JavaScript application, you may not need the full Chroma library. Instead, you can use the lightweight client-only library.

In this case, you can install the `chromadb-client` package **instead** of our `chromadb` package.

The `chromadb-client` package is a lightweight HTTP client for the server with a minimal dependency footprint.


```terminal
# Python
pip install chromadb-client
# JS
npm install chromadb-client
```

```python
# Python
import chromadb
# Example setup of the client to connect to your chroma server
client = chromadb.HttpClient(host='localhost', port=8000)

# Or for async usage:
async def main():
    client = await chromadb.AsyncHttpClient(host='localhost', port=8000)
```

```javascript
// JavaScript
import { ChromaClient } from "chromadb-client";
const client = new ChromaClient({ path: "http://localhost:8000" })
```

Note that the `chromadb-client` package is a subset of the full Chroma library and does not include all the dependencies. If you want to use the full Chroma library, you can install the `chromadb` package instead.

Most importantly, the thin-client package has no default embedding functions. If you `add()` documents without embeddings, you must have manually specified an embedding function and installed the dependencies for it.