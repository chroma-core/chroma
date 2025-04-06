from concurrent.futures import ThreadPoolExecutor
import os
import multiprocessing
from typing import List, Any, Dict
from tqdm import tqdm
from chromadb import Collection

def collection_add_in_batches(
    collection: Collection, 
    ids: List[str], 
    texts: List[str], 
    embeddings: List[List[float]], 
    metadatas: List[Dict] = None
) -> None:
    BATCH_SIZE = 100
    LEN = len(embeddings)
    N_THREADS = min(os.cpu_count() or multiprocessing.cpu_count(), 20)

    def add_batch(start: int, end: int) -> None:
        id_batch = ids[start:end]
        doc_batch = texts[start:end]

        print(f"Adding {start} to {end}")

        try:
            if metadatas:
                collection.add(ids=id_batch, documents=doc_batch, embeddings=embeddings[start:end], metadatas=metadatas[start:end])
            else:
                collection.add(ids=id_batch, documents=doc_batch, embeddings=embeddings[start:end])
        except Exception as e:
            print(f"Error adding {start} to {end}")
            print(e)

    threadpool = ThreadPoolExecutor(max_workers=N_THREADS)

    for i in range(0, LEN, BATCH_SIZE):
        threadpool.submit(add_batch, i, min(i + BATCH_SIZE, LEN))

    threadpool.shutdown(wait=True)

def get_collection_items(
    collection: Collection,
) -> Dict:
    BATCH_SIZE = 100
    collection_size = collection.count()
    items = collection.get(include=["metadatas"])

    ids = items['ids']

    embeddings_lookup = dict()

    for i in tqdm(range(0, collection_size, BATCH_SIZE), desc="Processing batches"):
        batch_ids = ids[i:i + BATCH_SIZE]
        result = collection.get(ids=batch_ids, include=["embeddings", "documents"])

        retrieved_ids = result["ids"]
        retrieved_embeddings = result["embeddings"]
        retrieved_documents = result["documents"]

        for id, embedding, document in zip(retrieved_ids, retrieved_embeddings, retrieved_documents):
            embeddings_lookup[id] = {
                'embedding': embedding,
                'document': document
            }
        
    return embeddings_lookup