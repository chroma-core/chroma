"""
This file starts a web server and hosts the code search API.
"""
from flask import Flask, request
from flask_cors import CORS

import inspect
import util
import main

app = Flask(__name__)
# CORS must be enabled so the website can access the API on localhost
CORS(app)


@app.route("/api/health")
def health():
    return {"result": "Hello from the Chroma code search sample app!"}


@app.route("/api/state")
def state():
    chunking_function = inspect.getsource(main.chunking)
    embedding_function = inspect.getsource(main.embedding_function)
    query_function = inspect.getsource(main.query)
    source_code = f"""{chunking_function}\n\n{embedding_function}\n\n{query_function}"""
    collection_name = main.collection.name
    chunk_count = main.collection.count()
    return {
        "result": {
            "source_code": source_code,
            "chunk_count": chunk_count,
            "collection_name": collection_name,
        }
    }


@app.route("/api/query")
def query():
    q = request.args.get("q")
    if q is None:
        return {"error": "Missing query parameter"}, 400
    parsed_query = util.parse_query(q)
    try:
        results = main.query(parsed_query)
    except Exception as e:
        print(e)
        return {"error": str(e)}, 500
    if len(results) > 5000:
        return {
            "error": f"Warning: the server just tried to send you {len(results)} results. Try limiting the number of results returned."
        }, 500
    return {"result": results}, 200


if __name__ == "__main__":
    # Apple took default port 5000 -- use this port instead
    app.run(debug=True, port=3001)
