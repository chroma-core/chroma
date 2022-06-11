from curses.ascii import EM

from ariadne import convert_kwargs_to_snake_case

from .models import Embedding


def resolve_embeddings(obj, info):
    try:
        embeddings = [emb.to_dict() for emb in Embedding.query.all()]
        payload = {"success": True, "embeddings": embeddings}
    except Exception as error:
        payload = {"success": False, "errors": [str(error)]}
    return payload


@convert_kwargs_to_snake_case
def resolve_embedding(obj, info, embedding_id):
    try:
        embedding = Embedding.query.get(embedding_id)
        payload = {"success": True, "embedding": embedding.to_dict()}
    except Exception:
        payload = {"success": False, "errors": [f"Embedding with id {embedding_id} not found"]}
    return payload
