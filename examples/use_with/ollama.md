# Ollama

First let's run a local docker container with Ollama. We'll pull `llama2` model:

```bash
docker run -d -v ./ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
docker exec -it ollama ollama run llama2 # press Ctrl+D to exit after model downloads successfully
# test it
curl http://localhost:11434/api/embeddings -d '{\n  "model": "llama2",\n  "prompt": "Here is an article about llamas..."\n}'
```

Now let's configure our OllamaEmbeddingFunction Embedding function with custom endpoint:

```python
import chromadb
from chromadb.utils.embedding_functions import OllamaEmbeddingFunction

client = chromadb.PersistentClient(path="ollama")

# create EF with custom endpoint
ef = OllamaEmbeddingFunction(
    model_name="llama2",
    url="http://localhost:11434/api/embeddings",
)

print(ef("Here is an article about llamas..."))
```
