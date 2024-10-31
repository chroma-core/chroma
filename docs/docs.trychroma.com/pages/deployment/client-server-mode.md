---
title: "Running Chroma in Client-Server Mode"
---

{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

Chroma can also be configured to run in client/server mode. In this mode, the Chroma client connects to a Chroma server running in a separate process.

This means that you can deploy single-node Chroma to a [Docker container](./docker), or a machine hosted by a cloud provider like [AWS](./aws), GCP, Azure, and others. Then, you can access your Chroma server from your application using our `HttpClient`.

You can quickly experiment locally with Chroma in client/server mode by using our CLI:

```shell
chroma run --path /db_path
```

Then use the Chroma `HttpClient` to connect to the server:

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

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

If you intend to deploy your Chroma server, you may want to consider our [thin-client package](./thin-client) for client-side interactions.

{% /tab %}

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Javascript" %}

```javascript
// CJS
const { ChromaClient } = require("chromadb");
// ESM
import { ChromaClient } from "chromadb";
const client = new ChromaClient();
```

{% /tab %}
