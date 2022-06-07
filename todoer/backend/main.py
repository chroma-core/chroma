import os
from os.path import isfile, getsize
from api import app, db, db_uri
from api import models
from ariadne import load_schema_from_path, make_executable_schema, \
    graphql_sync, snake_case_fallback_resolvers, ObjectType
from ariadne.constants import PLAYGROUND_HTML
from flask import request, jsonify, render_template
from api.queries import resolve_todos, resolve_todo, resolve_embeddings
from api.mutations import resolve_create_todo, resolve_mark_done, \
    resolve_delete_todo, resolve_update_due_date

def isSQLite3(filename):
    print(os.getcwd())
    print(filename)
    if not isfile(filename):
        return False
    if getsize(filename) < 100: # SQLite database file header is 100 bytes
        return False

    with open(filename, 'rb') as fd:
        header = fd.read(100)

    return header[:16].decode("utf-8") == 'SQLite format 3\x00'

if not isSQLite3('todo.db'):
  db.create_all()
  print('No DB existed. Created DB.')
else:
  print('DB in place')

# setting up graphql "routes"
query = ObjectType("Query")
query.set_field("todos", resolve_todos)
query.set_field("todo", resolve_todo)
query.set_field("embeddings", resolve_embeddings)

mutation = ObjectType("Mutation")
mutation.set_field("createTodo", resolve_create_todo)
mutation.set_field("markDone", resolve_mark_done)
mutation.set_field("deleteTodo", resolve_delete_todo)
mutation.set_field("updateDueDate", resolve_update_due_date)

type_defs = load_schema_from_path("schema.graphql")
schema = make_executable_schema(
    type_defs, query, mutation, snake_case_fallback_resolvers
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

    success, result = graphql_sync(
        schema,
        data,
        context_value=request,
        debug=app.debug
    )

    status_code = 200 if success else 400
    return jsonify(result), status_code