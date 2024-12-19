# Install and Run

You can install the Chroma CLI with `pip`:

```terminal
pip install chromadb
```

You can then run a Chroma server locally with the `chroma run` command:

```terminal
chroma run --path [/path/to/persist/data]
```

Your Chroma server will persist its data in the path you provide after the `path` argument. By default, 
it will save data to the `.chroma` directory.

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
