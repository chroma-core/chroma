from vars import CHROMA_COLLECTION_NAME, MAX_CHROMA_BATCH_SIZE

import itertools
import json
import uuid

import chromadb
from chromadb.types import Metadata
import util

client = chromadb.PersistentClient()

ef = util.get_embedding_function()

code_collection = client.get_or_create_collection(
    name=CHROMA_COLLECTION_NAME,
    embedding_function=ef
    #embedding_function=SentenceTransformerEmbeddingFunction(model_name="jinaai/jina-embeddings-v2-base-code")
)

def add_to_chroma(data: list[dict]) -> None:
    assert len(data) <= MAX_CHROMA_BATCH_SIZE
    documents: list[str] = [json_obj.pop('code') for json_obj in data]
    metadatas: list[Metadata] = [metadata_filter_keys(metadata) for metadata in data]
    ids: list[str] = [str(uuid.uuid4()) for _ in range(len(data))]

    code_collection.add(
        documents=documents,
        metadatas=metadatas,
        ids=ids
    )

def metadata_filter_keys(metadata: dict) -> dict:
    return {k: metadata[k] for k in ['repo', 'path', 'func_name', 'language', 'docstring', 'url'] if k in metadata}

def load_data():
    for lang in ['python']:
        print(lang)
        file_path = f'data/CodeSearchNet/{lang}/test.jsonl'
        max_batch_size = 10
        batch_count = 0
        with open(file_path, 'r') as file:
            for lines in itertools.batched(file, max_batch_size):
                print(f'===PROCESSING CHROMA BATCH {batch_count}===')
                json_objs = [json.loads(line) for line in lines]
                add_to_chroma(json_objs)
                batch_count += 1

def benchmark():
    successful, total = 0, 0
    data = code_collection.get()
    assert data['documents'] != None and data['metadatas'] != None
    for id, document, metadata in zip(data['ids'], data['documents'], data['metadatas']):
        assert type(metadata['docstring']) == str
        res = code_collection.query(query_texts=[metadata['docstring']], n_results=100)
        if id in res['ids'][0]:
            successful += 1
        total += 1
        print(f"{successful / total:.3f}")

if __name__ == '__main__':
    load_data()
else:
    raise Exception("This module should not be imported")
