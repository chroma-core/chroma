from chromadb.api.types import Embeddings

def convert_np_embeddings_to_list(embeddings: Embeddings) -> Embeddings:
    return [embedding.tolist() for embedding in embeddings]