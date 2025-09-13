---
id: integrations
name: Integrations
---

# Integrations

### Embedding Integrations

Embeddings are the A.I-native way to represent any kind of data, making them the perfect fit for working with all kinds of A.I-powered tools and algorithms. They can represent text, images, and soon audio and video. There are many options for creating embeddings, whether locally using an installed library, or by calling an API.

Chroma provides lightweight wrappers around popular embedding providers, making it easy to use them in your apps. You can set an embedding function when you create a Chroma collection, which will be used automatically, or you can call them directly yourself.

{% special_table %}
{% /special_table %}

|                                                                         | Python | Typescript |
| ----------------------------------------------------------------------- | ------ | ---------- |
| [OpenAI](./embedding-models/openai)                                     | ✓      | ✓          |
| [Google Gemini](./embedding-models/google-gemini)                       | ✓      | ✓          |
| [Cohere](./embedding-models/cohere)                                     | ✓      | ✓          |
| [Baseten](./embedding-models/baseten)                                   | ✓      | -          |
| [Hugging Face](./embedding-models/hugging-face)                         | ✓      | -          |
| [Instructor](./embedding-models/instructor)                             | ✓      | -          |
| [Hugging Face Embedding Server](./embedding-models/hugging-face-server) | ✓      | ✓          |
| [Jina AI](./embedding-models/jina-ai)                                   | ✓      | ✓          |
| [Roboflow](./embedding-models/roboflow)                                 | ✓      | -          |
| [Ollama Embeddings](./embedding-models/ollama)                          | ✓      | ✓          |
| [Cloudflare Workers AI](./embedding-models/cloudflare-workers-ai.md)    | ✓      | ✓          |
| [Together AI](./embedding-models/together-ai.md)                        | ✓      | ✓          |
| [Mistral](./embedding-models/mistral.md)                                | ✓      | ✓          |
| [Morph](./embedding-models/morph.md)                                    | ✓      | ✓          |

---

### Framework Integrations

Chroma maintains integrations with many popular tools. These tools can be used to define the business logic of an AI-native application, curate data, fine-tune embedding spaces and more.

We welcome pull requests to add new Integrations to the community.

{% special_table %}
{% /special_table %}

|                                             | Python | JS           |
| ------------------------------------------- | ------ | ------------ |
| [DeepEval](./frameworks/deepeval)           | ✓      | -            |
| [Langchain](./frameworks/langchain)         | ✓      | ✓            |
| [LlamaIndex](./frameworks/llamaindex)       | ✓      | ✓            |
| [Braintrust](./frameworks/braintrust)       | ✓      | ✓            |
| [OpenLLMetry](./frameworks/openllmetry)     | ✓      | Coming Soon! |
| [Streamlit](./frameworks/streamlit)         | ✓      | -            |
| [Haystack](./frameworks/haystack)           | ✓      | -            |
| [OpenLIT](./frameworks/openlit)             | ✓      | Coming Soon! |
| [Anthropic MCP](./frameworks/anthropic-mcp) | ✓      | Coming Soon! |
| [VoltAgent](./frameworks/voltagent)         | -      | ✓            |
| [Mem0](./frameworks/mem0)                   | ✓      | -            |
