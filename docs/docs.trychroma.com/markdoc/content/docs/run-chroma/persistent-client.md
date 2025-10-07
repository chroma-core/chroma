---
id: persistent-client
name: Persistent Client
---

# Persistent Client

{% Tabs %}

{% Tab label="python" %}

You can configure Chroma to save and load the database from your local machine, using the `PersistentClient`.

Data will be persisted automatically and loaded on start (if it exists).

```python
import chromadb

client = chromadb.PersistentClient(path="/path/to/save/to")
```

The `path` is where Chroma will store its database files on disk, and load them on start. If you don't provide a path, the default is `.chroma`

{% /Tab %}

{% Tab label="typescript" %}

To connect with the JS/TS client, you must connect to a Chroma server.

To run a Chroma server locally that will persist your data, install Chroma from npm using any npm compatible client.

```terminal
npm install chromadb
```

And run the server using our CLI:

```terminal
npx chroma run --path ./getting-started
```

The `path` is where Chroma will store its database files on disk, and load them on start. The default is `.chroma`.

Alternatively, you can also use our official Docker image:

```terminal
docker pull chromadb/chroma
docker run -p 8000:8000 chromadb/chroma
```

With a Chroma server running locally, you can connect to it by instantiating a new `ChromaClient`:

```typescript
import { ChromaClient } from "chromadb";

const client = new ChromaClient();
```

By default, the `ChromaClient` is wired to connect to a Chroma server at `http://localhost:8000`, with `default_tenant` and `default_database`. If you have different settings you can provide them to the `ChromaClient` constructor:

```typescript
const client = new ChromaClient({
  ssl: false,
  host: "localhost",
  port: 9000, // non-standard port based on your server config
  database: "my-db",
  headers: {},
});
```

{% /Tab %}

{% /Tabs %}

The client object has a few useful convenience methods.

- `heartbeat()` - returns a nanosecond heartbeat. Useful for making sure the client remains connected.
- `reset()` - empties and completely resets the database. ⚠️ This is destructive and not reversible.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
client.heartbeat()
client.reset()
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await client.heartbeat();
await client.reset();
```

{% /Tab %}

{% /TabbedCodeBlock %}
