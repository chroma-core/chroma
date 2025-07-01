# Running Chroma in Client-Server Mode

Chroma can also be configured to run in client/server mode. In this mode, the Chroma client connects to a Chroma server running in a separate process.

This means that you can deploy single-node Chroma to a [Docker container](../containers/docker), or a machine hosted by a cloud provider like [AWS](../cloud-providers/aws), [GCP](../cloud-providers/gcp), [Azure](../cloud-providers/azure), and others. Then, you can access your Chroma server from your application using our `HttpClient` (or `ChromaClient` for JS/TS users).

You can quickly experiment locally with Chroma in client/server mode by using our CLI:

```terminal
chroma run --path /db_path
```

{% Tabs %}

{% Tab label="python" %}

Then use the Chroma `HttpClient` to connect to the server:

```python
import chromadb
chroma_client = chromadb.HttpClient(host='localhost', port=8000)
```

Chroma also provides an `AsyncHttpClient`. The behaviors and method signatures are identical to the synchronous client, but all methods that would block are now async:

```python
import asyncio
import chromadb

async def main():
    client = await chromadb.AsyncHttpClient()
    collection = await client.create_collection(name="my_collection")
    await collection.add(
        documents=["hello world"],
        ids=["id1"]
    )

asyncio.run(main())
```

If you intend to deploy your Chroma server, you may want to consider our [thin-client package](/production/chroma-server/python-thin-client) for client-side interactions.

{% /Tab %}

{% Tab label="typescript" %}

Then instantiate a new `ChromaClient`. The default is to connect to a Chroma server running on localhost.

```typescript
// CJS
const { ChromaClient } = require("chromadb");
// ESM
import { ChromaClient } from "chromadb";

const client = new ChromaClient();
```

{% /Tab %}

{% /Tabs %}
