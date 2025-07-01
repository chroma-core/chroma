from typing import List, Any
from tqdm import tqdm
import requests
import json
from voyageai import Client as VoyageClient
from openai import OpenAI as OpenAIClient

def minilm_embed(
    model: Any,
    texts: List[str],
) -> List[List[float]]:
    embeddings = model.encode(texts)
    return embeddings

def minilm_embed_in_batches(
    model: Any, 
    texts: List[str], 
    batch_size: int = 100
) -> List[List[float]]:
    all_embeddings = []
    
    for i in tqdm(range(0, len(texts), batch_size), desc="Processing MiniLM batches"):
        batch = texts[i:i + batch_size]
        batch_embeddings = minilm_embed(model, batch)
        all_embeddings.extend(batch_embeddings)
    
    return all_embeddings


def openai_embed(
    openai_client: OpenAIClient, 
    texts: List[str], 
    model: str
) -> List[List[float]]:
    try:
        return [response.embedding for response in openai_client.embeddings.create(model=model, input = texts).data]
    except Exception as e:
        print(f"Error embedding: {e}")
        return [[0.0]*1024 for _ in texts]

def openai_embed_in_batches(
    openai_client: OpenAIClient, 
    texts: List[str], 
    model: str, 
    batch_size: int = 100
) -> List[List[float]]:
    all_embeddings = []

    for i in tqdm(range(0, len(texts), batch_size), desc="Processing OpenAI batches"):
        batch = texts[i:i + batch_size]
        batch_embeddings = openai_embed(openai_client, batch, model)
        all_embeddings.extend(batch_embeddings)

    return all_embeddings


def jina_embed(
    JINA_API_KEY: str, 
    input_type: str, 
    texts: List[str]
) -> List[List[float]]:
    try:
        url = "https://api.jina.ai/v1/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {JINA_API_KEY}"
        }
        
        data = {
            "model": "jina-embeddings-v3",
            "task": input_type,
            "late_chunking": False,
            "dimensions": 1024,
            "embedding_type": "float",
            "input": texts
        }

        response = requests.post(url, headers=headers, json=data)
        response_dict = json.loads(response.text)
        embeddings = [item["embedding"] for item in response_dict["data"]]
        
        return embeddings
    
    except Exception as e:
        print(f"Error embedding batch: {e}")
        return [[0.0]*1024 for _ in texts]

def jina_embed_in_batches(
    JINA_API_KEY: str, 
    input_type: str, 
    texts: List[str], 
    batch_size: int = 100
) -> List[List[float]]:
    all_embeddings = []
    
    for i in tqdm(range(0, len(texts), batch_size), desc="Processing Jina batches"):
        batch = texts[i:i + batch_size]
        batch_embeddings = jina_embed(JINA_API_KEY, input_type, batch)
        all_embeddings.extend(batch_embeddings)
    
    return all_embeddings


def voyage_embed(
    voyage_client: VoyageClient, 
    input_type: str, 
    texts: List[str]
) -> List[List[float]]:
    try:
        response = voyage_client.embed(texts, model="voyage-3-large", input_type=input_type)
        return response.embeddings
    
    except Exception as e:
        print(f"Error embedding batch: {e}")
        return [[0.0]*1024 for _ in texts]

def voyage_embed_in_batches(
    voyage_client: VoyageClient, 
    input_type: str, 
    texts: List[str], 
    batch_size: int = 100
) -> List[List[float]]:
    all_embeddings = []
    
    for i in tqdm(range(0, len(texts), batch_size), desc="Processing Voyage batches"):
        batch = texts[i:i + batch_size]

        batch_embeddings = voyage_embed(voyage_client, input_type, batch)

        all_embeddings.extend(batch_embeddings)
    
    return all_embeddings