# Code Search Sample App

Chunking strategies, embedding models, reranking methods, and more! I've been
researching how to build my own code search engine, but there's so many
different ways you can do it!

This repository contains a toolkit of functions that can be assembled into
code retrieval pipelines, and a benchmarking tool to see how they perform.

## Components

1. Models --
2. Chunking strategies -- we supply multiple
3. Search strategies
    1. Semantic search using ChromaDB
    2. Lexical-based searching using TF-IDF or BM25
4. Reranking strategies

## How to use

- Download a git repository using `python functions/load_data_from_github.py user/reponame`
- (Optional) Change the chunking method in `main.py/chunking`
- Chunk your codebase using `python functions/chunk_data.py user/reponame`
- (Optional) Change the embedding function used in `main.py/embedding_function`
- Embed and upload your data to your Chroma collection using `python functions/embed_and_upload.py user/reponame`
- (Optional) Change the retrieval pipeline used in `main.py/query`
-
