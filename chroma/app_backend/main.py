import os
from os.path import getsize, isfile

from api import app, db
from flask import request, jsonify, render_template
from ariadne.constants import PLAYGROUND_HTML
from ariadne import load_schema_from_path, make_executable_schema, \
    graphql_sync, snake_case_fallback_resolvers, ObjectType
from api.queries import resolve_datapoints

def isSQLite3(filename):
    print(os.getcwd())
    print(filename)
    if not isfile(filename):
        return False
    if getsize(filename) < 100:  # SQLite database file header is 100 bytes
        return False

    with open(filename, "rb") as fd:
        header = fd.read(100)

    return header[:16].decode("utf-8") == "SQLite format 3\x00"

if not isSQLite3("chroma.db"):
    db.create_all()
    print("No DB existed. Created DB.")
else:
    print("DB in place")

# setting up graphql "routes"
query = ObjectType("Query")
query.set_field("datapoints", resolve_datapoints)

mutation = ObjectType("Mutation")

app_backend_type_defs = load_schema_from_path("schema.graphql")

schema = make_executable_schema(
    app_backend_type_defs, query, mutation, snake_case_fallback_resolvers
)

@app.route("/")
def my_index():
    return render_template("index.html", flask_token="Hello world")


# graphql playground
@app.route("/graphql", methods=["GET"])
def graphql_playground():
    return PLAYGROUND_HTML, 200


# the one api endpoint for graphql
@app.route("/graphql", methods=["POST"])
def graphql_server():
    data = request.get_json()

    success, result = graphql_sync(schema, data, context_value=request, debug=app.debug)

    status_code = 200 if success else 400
    return jsonify(result), status_code
