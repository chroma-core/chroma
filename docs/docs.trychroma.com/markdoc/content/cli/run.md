# Running a Chroma Server

The Chroma CLI lets you run a Chroma server locally with the `chroma run` command:

```terminal
chroma run --path [/path/to/persist/data]
```

Your Chroma server will persist its data in the path you provide after the `path` argument. By default,
it will save data to the `chroma` directory.

You can further customize how your Chroma server runs with these arguments:
* `host` - defines the hostname where your server runs. By default, this is `localhost`.
* `port` - the port your Chroma server will use to listen for requests from clients. By default the port is `8000`.
* `config_path` - instead of providing `path`, `host`, and `port`, you can provide a configuration file with these definitions and more. You can find an example [here](https://github.com/chroma-core/chroma/blob/main/rust/frontend/sample_configs/single_node_full.yaml).

## Connecting to your Chroma Server

With your Chroma server running, you can connect to it with the `HttpClient`:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
import chromadb

chroma_client = chromadb.HttpClient(host='localhost', port=8000)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { ChromaClient } from "chromadb";

const client = new ChromaClient();
```
{% /Tab %}

{% /TabbedCodeBlock %}