# Embedding Functions

Embeddings are the way to represent any kind of data, making them the perfect fit for working with all kinds of A.I-powered tools and algorithms. They can represent text, images, and soon audio and video. There are many options for creating embeddings, whether locally using an installed library, or by calling an API.

Chroma provides lightweight wrappers around popular embedding providers, making it easy to use them in your apps. You can set an embedding function when you create a Chroma collection, which will be used automatically, or you can call them directly yourself.

|                                                                                          | Python | Typescript |
|------------------------------------------------------------------------------------------|--------|------------|
| [OpenAI](../../integrations/embedding-models/openai)                                     | ✓      | ✓          |
| [Google Generative AI](../../integrations/embedding-models/google-gemini)                | ✓      | ✓          |
| [Cohere](../../integrations/embedding-models/cohere)                                     | ✓      | ✓          |
| [Hugging Face](../../integrations/embedding-models/hugging-face)                         | ✓      | -          |
| [Instructor](../../integrations/embedding-models/instructor)                             | ✓      | -          |
| [Hugging Face Embedding Server](../../integrations/embedding-models/hugging-face-server) | ✓      | ✓          |
| [Jina AI](../../integrations/embedding-models/jina-ai)                                   | ✓      | ✓          |
| [Cloudflare Workers AI](../../integrations/embedding-models/cloudflare-workers-ai.md)    | ✓      | ✓          |
| [Together AI](../../integrations/embedding-models/together-ai.md)                        | ✓      | ✓          |
| [Mistral](../../integrations/embedding-models/mistral.md)                                | ✓      | -          |

We welcome pull requests to add new Embedding Functions to the community.

***

## Default: all-MiniLM-L6-v2

By default, Chroma uses the [Sentence Transformers](https://www.sbert.net/) `all-MiniLM-L6-v2` model to create embeddings. This embedding model can create sentence and document embeddings that can be used for a wide variety of tasks. This embedding function runs locally on your machine, and may require you download the model files (this will happen automatically).

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb.utils import embedding_functions
default_ef = embedding_functions.DefaultEmbeddingFunction()
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { DefaultEmbeddingFunction } from "chromadb";
const defaultEF = new DefaultEmbeddingFunction();
```
{% /Tab %}

{% /TabbedCodeBlock %}

Embedding functions can be linked to a collection and used whenever you call `add`, `update`, `upsert` or `query`. You can also use them directly which can be handy for debugging.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
val = default_ef(["foo"])
print(val) # [[0.05035809800028801, 0.0626462921500206, -0.061827320605516434...]]
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
const val = defaultEf.generate(["foo"]);
console.log(val); // [[0.05035809800028801, 0.0626462921500206, -0.061827320605516434...]]
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Sentence Transformers

Chroma can also use any [Sentence Transformers](https://www.sbert.net/) model to create embeddings.

You can pass in an optional `model_name` argument, which lets you choose which Sentence Transformers model to use. By default, Chroma uses `all-MiniLM-L6-v2`. You can see a list of all available models [here](https://www.sbert.net/docs/pretrained_models.html).

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2"
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { DefaultEmbeddingFunction } from "chromadb";
const modelName = "all-MiniLM-L6-v2";
const defaultEF = new DefaultEmbeddingFunction(modelName);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Custom Embedding Functions

You can create your own embedding function to use with Chroma, it just needs to implement the `EmbeddingFunction` protocol.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Documents, EmbeddingFunction, Embeddings

class MyEmbeddingFunction(EmbeddingFunction):
    def __call__(self, input: Documents) -> Embeddings:
        # embed the documents somehow
        return embeddings
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
class MyEmbeddingFunction {
    private api_key: string;

    constructor(api_key: string) {
        this.api_key = api_key;
    }

    public async generate(texts: string[]): Promise<number[][]> {
        // do things to turn texts into embeddings with an api_key perhaps
        return embeddings;
    }
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

We welcome contributions! If you create an embedding function that you think would be useful to others, please consider [submitting a pull request](https://github.com/chroma-core/chroma) to add it to Chroma's `embedding_functions` module.
