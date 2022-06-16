import os
from os.path import getsize, isfile

from ariadne import (
    ObjectType,
    graphql_sync,
    load_schema_from_path,
    make_executable_schema,
    snake_case_fallback_resolvers,
)
from ariadne.constants import PLAYGROUND_HTML
from flask import jsonify, request

from chroma.data_manager.api import CHROMA_DATAMANAGER_DB_NAME, app, db
from chroma.data_manager.api.models import Embedding
from chroma.data_manager.api.mutations import (
    resolve_batch_create_embeddings,
    resolve_create_embedding,
    resolve_delete_embedding,
)
from chroma.data_manager.api.queries import resolve_embedding, resolve_embeddings


def isSQLite3(filename):
    if not isfile(filename):
        return False
    if getsize(filename) < 100:  # SQLite database file header is 100 bytes
        return False

    with open(filename, "rb") as fd:
        header = fd.read(100)

    return header[:16].decode("utf-8") == "SQLite format 3\x00"


if not isSQLite3(CHROMA_DATAMANAGER_DB_NAME):
    db.create_all()
    print(" * No DB existed. Created DB.")
else:
    print(" * DB in place")
    if app.env == "dev_from_scratch":
        print(" * Starting from scratch - clearing DB")
        deletion = Embedding.query.delete()
        db.session.commit()

query = ObjectType("Query")

query.set_field("embeddings", resolve_embeddings)
query.set_field("embedding", resolve_embedding)

mutation = ObjectType("Mutation")
mutation.set_field("createEmbedding", resolve_create_embedding)
mutation.set_field("batchCreateEmbeddings", resolve_batch_create_embeddings)
mutation.set_field("deleteEmbedding", resolve_delete_embedding)

type_defs = load_schema_from_path("schema.graphql")
schema = make_executable_schema(type_defs, query, mutation, snake_case_fallback_resolvers)


@app.route("/graphql", methods=["GET"])
def graphql_playground():
    return PLAYGROUND_HTML, 200


@app.route("/graphql", methods=["POST"])
def graphql_server():
    data = request.get_json()

    success, result = graphql_sync(schema, data, context_value=request, debug=app.debug)

    status_code = 200 if success else 400
    return jsonify(result), status_code
