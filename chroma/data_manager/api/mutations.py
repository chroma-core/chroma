import json

from ariadne import convert_kwargs_to_snake_case

from chroma.data_manager.api import db
from chroma.data_manager.api.models import Embedding


@convert_kwargs_to_snake_case
def resolve_create_embedding(obj, info, data, input_identifier, inference_identifier, label):
    try:
        emb = Embedding(
            data=json.dumps(data),
            input_identifier=input_identifier,
            inference_identifier=inference_identifier,
            label=label,
        )
        db.session.add(emb)
        db.session.commit()
        payload = {"success": True, "embedding": emb.to_dict()}
    except ValueError as error:
        payload = {"success": False, "errors": [str(error)]}

    return payload


@convert_kwargs_to_snake_case
def resolve_batch_create_embeddings(obj, info, data, input_identifiers, inference_identifiers, labels):
    try:
        annotated_data = zip(data, input_identifiers, inference_identifiers, labels)

        batch_embs = [
            Embedding(data=json.dumps(datum), input_identifier=input_identifier, inference_identifier=inference_identifier, label=label)
            for datum, input_identifier, inference_identifier, label in annotated_data
        ]
        db.session.bulk_save_objects(batch_embs)
        db.session.commit()

        payload = {
            "success": True,
        }
    except ValueError as error:
        payload = {"success": False, "errors": [str(error)]}

    return payload


@convert_kwargs_to_snake_case
def resolve_delete_embedding(obj, info, embedding_id):
    try:
        embedding = Embedding.query.get(embedding_id)
    except Exception:
        payload = {"success": False, "errors": [f"Embedding matching id {embedding_id} not found"]}
        return payload

    try:
        db.session.delete(embedding)
        db.session.commit()
        payload = {"success": True}
    except Exception:
        payload = {"success": False, "errors": [f"Embedding matching id {embedding_id} not found"]}

    return payload
