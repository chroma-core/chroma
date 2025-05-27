"""
Use this script to upload your local data to a Chroma collection
Each line in this file is an entry in the dataset
Look at data/chroma_docs.txt for an example of the format
"""

import os
import uuid
import dotenv

from openai import OpenAI as OpenAIClient
from functions.chroma import collection_add_in_batches
from functions.embed import openai_embed_in_batches
import chromadb

dotenv.load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai_client = OpenAIClient(api_key=OPENAI_API_KEY)

# CHANGE ME ###################################################################
DATASET_FILE = "data/chroma_docs.txt"
CHROMA_COLLECTION_NAME = "generative-benchmarking-custom-data"


def embed(documents: list[str]) -> list[list[float]]:
    # We provide helper functions to use OpenAI, Voyage, and Jina, and
    # more! Check out functions/embed.py and replace this function with your own!
    return openai_embed_in_batches(
        openai_client=openai_client,
        texts=documents,
        model="text-embedding-3-large",
    )


# CHANGE ME ###################################################################

assert os.path.exists(DATASET_FILE), "Check the value of DATASET_FILE"

CHROMA_CLOUD_API_KEY = os.getenv("CHROMA_CLOUD_API_KEY") or ""
CHROMA_HOST = os.getenv("CHROMA_HOST")
CHROMA_TENANT = os.getenv("CHROMA_TENANT")
CHROMA_DB_NAME = os.getenv("CHROMA_DB_NAME")

assert CHROMA_HOST, "Check the value of CHROMA_HOST in .env"
assert CHROMA_TENANT, "Check the value of CHROMA_TENANT in .env"
assert CHROMA_DB_NAME, "Check the value of CHROMA_DB_NAME in .env"

using_local_chroma = CHROMA_HOST == "localhost"

chroma_client = chromadb.HttpClient(
    ssl=not using_local_chroma,
    host=CHROMA_HOST,
    tenant=CHROMA_TENANT,
    database=CHROMA_DB_NAME,
    headers={"x-chroma-token": CHROMA_CLOUD_API_KEY},
)

corpus_collection = chroma_client.get_or_create_collection(
    name=CHROMA_COLLECTION_NAME, metadata={"hnsw:space": "cosine"}
)

with open(DATASET_FILE) as f:
    corpus_documents = f.readlines()
    corpus_ids = [str(uuid.uuid4()) for _ in range(len(corpus_documents))]
    corpus_embeddings = embed(corpus_documents)
    collection_add_in_batches(
        collection=corpus_collection,
        ids=corpus_ids,
        texts=corpus_documents,
        embeddings=corpus_embeddings,
    )
