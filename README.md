
![logo](https://user-images.githubusercontent.com/891664/218319391-75785e46-032d-4aef-b19f-b5c6f039d0a8.png)

## Chroma

__Chroma is the open-source embedding database__. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs. 

- [💬 Community Discord]()
- [📖 Documentation]()
- [💡 Colab Example]()
- [🏠 Homepage]()

## ChatGPT for ______

For example, the "`Chat your data`" use case:
1. Add documents to your database. You can pass in your own embeddings, embedding function, or let Chroma embed them for you.
2. Query relevant documents with natural language.
3. Compose documents into the context window of an LLM like `GTP3` for additional summarization or analysis. 


## Features
- __Simple__: Fully typed, fully tested, fully documented == happiness
- __Integrations__: `🦜️🔗 Langchain` and `🦙 gpt-index`
- __Dev, Test, Prod__: the same API runs in your python notebook and up to a cluster
- __Feature-rich__: Queries, filtering, density estimation and more
- __Fast__: 50-100x faster than other popular solutions
- __Free__: Apache 2.0 Licensed

## Get up and running
```python
pip install chromadb
```

```python 
import chromadb
client = chromadb.Client()
collection = client.create_collection("all-my-documents")
collection.add(
    embeddings=[[1.5, 2.9, 3.4], [9.8, 2.3, 2.9]],
    metadatas=[{"source": "notion"}, {"source": "google-docs"}],
    ids=["n/102", "gd/972"],
)
results = collection.query(
    query_texts=["How do I do ..."],
    n_results=3
)
```

## Get involved
Chroma is a rapidly developing project. We welcome PR contributors and ideas for how to improve the project. 
- [Join the conversation on Discord]()
- [Review the roadmap and contribute your ideas]()
- [Grab an issue and open a PR]()

## Embeddings?
What are embeddings?
- [Read the guide from OpenAI](https://platform.openai.com/docs/guides/embeddings/what-are-embeddings)
- __Literal__: Embedding something turns it from image/text/audio into a list of numbers. 🖼️/📄 => `[1.2, 2.1, ....]`. This process makes documents "understandable" to a machine learning model. 
- __By analogy__: An embedding represents the essence of a document. This enables documents and queries with the same essence to be "near" each other and therefore easy to find. 
- __Technical__: An embedding is the latent-space position of a document at a layer of a deep neural network. For models trained specifically to embed data, this is the last layer.
- __A small example__: If you search your photos for "famous bridge in San Francisco". Through embedding the photo and it's metadata - it should return photos of the Golden Gate Bridge.


## License

[Apache 2.0](./LICENSE)