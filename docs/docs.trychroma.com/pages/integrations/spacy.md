---
---

# spaCy

Pre-trained embeddings that are available from [spaCy](https://spacy.io/models/) can be used for encoding text into vectors. They are fast, robust and good alternative for a lot of language models. To use spacy models in embedding function we have to install spacy module and also download a model of our choice. Please use the below snippet to install and download a model of our choice.

```bash
pip install spacy
```

```bash
spacy download model_name
```

For the list models please visit: [spacy-models](https://spacy.io/models/)

```python
import chromadb.utils.embedding_functions as embedding_functions
ef = embedding_functions.SpacyEmbeddingFunction(model_name="en_core_web_md")
embeddings = ef(["text-1", "text-2"])
```
