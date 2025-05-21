"""
This file starts a web server and hosts the code search API.
"""
from flask import Flask, request
from flask_cors import CORS

from modules import search
import util

app = Flask(__name__)
# CORS must be enabled so the website can access the API on localhost
CORS(app)

@app.route("/api/health")
def health():
    return {"result": "Hello from the Chroma code search sample app!"}

@app.route("/api/state")
def state():
    return {"result": util.get_state()}

@app.route("/api/query")
def query():
    q = request.args.get("q")
    if q is None:
        return {"error": "Missing query parameter"}, 400
    parsed_query = util.parse_query(q)
    try:
        results = search.semantic_search_using_chroma(parsed_query)
    except Exception as e:
        print(e)
        return {"error": str(e)}, 500
    if len(results) > 5000:
        return {"error": f"Warning: the server just tried to send you {len(results)} results. Try limiting the number of results returned."}
    return {"result": results}

if __name__ == '__main__':
    # Apple took default port 5000 -- use this port instead
    app.run(debug=True, port=3001)
