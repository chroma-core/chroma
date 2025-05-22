"""
Use this script to upload your local data to a Chroma collection
Each line in this file is an entry in the dataset
"""

import os
import uuid
import dotenv

from openai import OpenAI as OpenAIClient
from functions.chroma import collection_add_in_batches
from functions.embed import openai_embed_in_batches
import chromadb

dotenv.load_dotenv()

DATASET_FILE = "data/your_data.txt"
CHROMA_COLLECTION_NAME = "generative-benchmarking-custom-data"

assert os.path.exists(DATASET_FILE)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CHROMA_CLOUD_API_KEY = os.getenv("CHROMA_CLOUD_API_KEY")
CHROMA_HOST = os.getenv("CHROMA_HOST")
CHROMA_TENANT = os.getenv("CHROMA_TENANT")
CHROMA_DB_NAME = os.getenv("CHROMA_DB_NAME")

chroma_client = chromadb.HttpClient(
    ssl=True,
    host=CHROMA_HOST,
    tenant=CHROMA_TENANT,
    database=CHROMA_DB_NAME,
    headers={"x-chroma-token": CHROMA_CLOUD_API_KEY},
)

corpus_collection = chroma_client.get_or_create_collection(
    name=CHROMA_COLLECTION_NAME, metadata={"hnsw:space": "cosine"}
)

openai_client = OpenAIClient(api_key=OPENAI_API_KEY)

with open(DATASET_FILE) as f:
    corpus_documents = f.readlines()
    corpus_ids = [str(uuid.uuid4()) for u in range(len(corpus_documents))]
    corpus_embeddings = openai_embed_in_batches(
        openai_client=openai_client,
        texts=corpus_documents,
        model="text-embedding-3-large",
    )
    collection_add_in_batches(
        collection=corpus_collection,
        ids=corpus_ids,
        texts=corpus_documents,
        embeddings=corpus_embeddings,
    )
