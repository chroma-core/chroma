````markdown
## Examples

> Searching for community contributions! Join the [#contributing](https://discord.com/channels/1073293645303795742/1074711539724058635) Discord Channel to discuss.

This folder will contain an ever-growing set of examples.

The key with examples is that they should _always_ work. The failure mode of examples folders is that they get quickly deprecated.

Examples are:

- Easy to maintain
- Easy to maintain examples are **simple**
- Use case examples are fine, technology is better

```
folder structure
- basic_functionality - notebooks with simple walkthroughs
- advanced_functionality - notebooks with advanced walkthroughs
- deployments - how to deploy places
- use_with - chroma + ___, where ___ can be langchain, nextjs, etc
- data - common data for examples
```

> 💡 Feel free to open a PR with an example you would like to see

### Basic Functionality

- [x] Examples of using different embedding models
- [x] Local persistance demo
- [x] Where filtering demo

### Advanced Functionality

- [ ] Clustering
- [ ] Projections
- [ ] Fine tuning

### Use With

#### LLM Application Code

- [ ] Langchain
- [ ] LlamaIndex
- [ ] Semantic Kernal

#### App Frameworks

- [ ] Streamlit
- [ ] Gradio
- [ ] Nextjs
- [ ] Rails
- [ ] FastAPI

#### Inference Services

- [ ] Brev.dev
- [ ] Banana.dev
- [ ] Modal

### LLM providers/services

- [ ] OpenAI
- [ ] Anthropic
- [ ] Cohere
- [ ] Google PaLM
- [ ] Hugging Face

---

### Inspiration

- The [OpenAI Cookbook](https://github.com/openai/openai-cookbook) gets a lot of things right
````

## local_simple_hash example (quick smoke test)

A tiny, dependency-free script demonstrating the `local_simple_hash` embedding function is included at:

`examples/local_simple_hash_example.py`

How to run (PowerShell):

```powershell
# from the repository root, in an activated venv
python -m pip install -e .
python examples/local_simple_hash_example.py
```

Expected output (example):

```
Embeddings from SimpleHashEmbeddingFunction:
doc 0: len=16, dtype=float32, norm=0.123456
doc 1: len=16, dtype=float32, norm=0.234567
doc 2: len=16, dtype=float32, norm=0.000000
doc 3: len=16, dtype=float32, norm=0.345678

Embeddings from config-built function:
len=16, dtype=float32, norm=0.234567
```

Notes

- The `local_simple_hash` function is deterministic and fast; it is intended for smoke tests and examples, not high-quality semantic similarity.
- Non-string inputs are accepted and stringified by the example script (useful when quickly testing inputs or fixtures).
- For higher-quality local embeddings, see `SentenceTransformerEmbeddingFunction` in `chromadb/utils/embedding_functions` (requires `sentence_transformers`).
