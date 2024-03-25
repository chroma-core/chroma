# Ollama

First let's run a local docker container with Ollama. We'll pull `nomic-embed-text` model:

```bash
docker run -d -v ./ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
docker exec -it ollama ollama run nomic-embed-text # press Ctrl+D to exit after model downloads successfully
# test it
curl http://localhost:11434/api/embeddings -d '{"model": "nomic-embed-text","prompt": "Here is an article about llamas..."}'
```

Now let's configure our OllamaEmbeddingFunction Embedding (python) function with the default Ollama endpoint:

```python
import chromadb
from chromadb.utils.embedding_functions import OllamaEmbeddingFunction

client = chromadb.PersistentClient(path="ollama")

# create EF with custom endpoint
ef = OllamaEmbeddingFunction(
    model_name="nomic-embed-text",
    url="http://127.0.0.1:11434/api/embeddings",
)

print(ef(["Here is an article about llamas..."]))
```

For JS users, you can use the `OllamaEmbeddingFunction` class to create embeddings:

```javascript
const {OllamaEmbeddingFunction} = require('chromadb');
const embedder = new OllamaEmbeddingFunction({
    url: "http://127.0.0.1:11434/api/embeddings",
    model: "llama2"
})

// use directly
const embeddings = embedder.generate(["Here is an article about llamas..."])
```
