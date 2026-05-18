# RAG Pipeline with Overlapping Text Chunking

A practical example of building a Retrieval-Augmented Generation (RAG) pipeline using ChromaDB with overlapping text chunking.

## What this covers

- Splitting long documents into overlapping chunks to preserve context at boundaries
- Embedding and storing chunks with source metadata in a ChromaDB collection
- Querying with semantic search and using results as LLM context

## Why overlapping chunks?

When you split a document into fixed-size chunks, sentences at the boundaries get cut in half. Overlapping by 100-200 characters means those boundary sentences appear fully in at least one chunk, which improves retrieval accuracy.

## Usage

```bash
pip install chromadb
python rag_chunking_example.py
```

## Adapting for production

- Swap the default embedding function for OpenAI or Cohere embeddings for better accuracy
- Use `chromadb.PersistentClient()` instead of `chromadb.Client()` to persist data to disk
- Add sentence-boundary-aware chunking instead of fixed character splits
- Pass retrieved chunks as context to an LLM for generation
