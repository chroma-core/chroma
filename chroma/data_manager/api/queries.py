from ariadne import convert_kwargs_to_snake_case

from .models import Embedding, EMBEDDING_PAGE_SIZE

@convert_kwargs_to_snake_case
def resolve_embeddings(obj, info):
    try:
        query_result = Embedding.query.all()
        embeddings = [emb.to_dict() for emb in query_result]
        payload = {"success": True, "embeddings": embeddings}
    except Exception as error:
        payload = {"success": False, "errors": [str(error)]}
    return payload

@convert_kwargs_to_snake_case
def resolve_embeddings_page(obj, info, index):
    try:
        page_offset = index*EMBEDDING_PAGE_SIZE
        query_result = Embedding.query.order_by(Embedding.id).offset(page_offset).limit(EMBEDDING_PAGE_SIZE).all()
        at_end = len(query_result) < EMBEDDING_PAGE_SIZE
        embeddings = [emb.to_dict() for emb in query_result]
        payload = {"success": True, "embeddings": embeddings, "at_end": at_end}
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
