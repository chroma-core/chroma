# Persistent Client

{% Tabs %}

{% Tab label="python" %}

You can configure Chroma to save and load the database from your local machine, using the `PersistentClient`. 

Data will be persisted automatically and loaded on start (if it exists).

```python
import chromadb

client = chromadb.PersistentClient(path="/path/to/save/to")
```

The `path` is where Chroma will store its database files on disk, and load them on start.

{% /Tab %}

{% Tab label="typescript" %}

To connect with the JS/TS client, you must connect to a Chroma server. 

To run a Chroma server locally that will persist your data, install Chroma via `pip`:

```terminal
pip install chromadb
```

And run the server using our CLI:

```terminal
chroma run --path ./getting-started 
```

The `path` is where Chroma will store its database files on disk, and load them on start.

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

See [Running Chroma in client-server mode](../client-server-mode) for more.

{% /Tab %}

{% /Tabs %}

The client object has a few useful convenience methods.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
client.heartbeat() # returns a nanosecond heartbeat. Useful for making sure the client remains connected.
client.reset() # Empties and completely resets the database. ⚠️ This is destructive and not reversible.
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await client.heartbeat(); // returns a nanosecond heartbeat. Useful for making sure the client remains connected.
await client.reset(); // Empties and completely resets the database. ⚠️ This is destructive and not reversible.
```
{% /Tab %}

{% /TabbedCodeBlock %}

