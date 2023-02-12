
![logo](https://user-images.githubusercontent.com/891664/218319391-75785e46-032d-4aef-b19f-b5c6f039d0a8.png)

## Chroma

__Chroma is the open-source embedding database__. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs. 

- [💬 Community Discord]()
- [📖 Documentation]()
- [💡 Colab Example]()
- [🏠 Homepage]()

## ChatGPT for ______

For example, the "`Chat your data`" use case:
1. Add documents (add your own embeddings, embedding function, or let Chroma embed them) to your database `collection.add`
2. Query relevant documents, `collection.query`



## Features
- __Simple__: Fully typed, fully tested, fully documented == happiness
- __Integrations__: `Langchain` and `gpt-index`
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
collection = client.create_collection("hello world")
collection.add(
    embeddings=[[1.5, 2.9, 3.4], [9.8, 2.3, 2.9]],
    metadatas=[{"style": "style1"}, {"style": "style2"}],
    ids=["uri9", "uri10"],
)
collection.query(
    query_embeddings=[[1.1, 2.3, 3.2], [5.1, 4.3, 2.2]],
    n_results=2,
    where={"style": "style2"}
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