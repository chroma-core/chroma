"""
This file starts a web server and hosts the code search API.
"""
from flask import Flask, request
from flask_cors import CORS

import inspect
import util
import main
from pathlib import Path
from vars import REPO_NAME, COMMIT_HASH

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
            "repo_name": main.REPO_NAME,
            "commit_hash": main.COMMIT_HASH,
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


@app.route("/api/file")
def get_file():
    """
    Get a file from the repository data.
    Uses the configured REPO_NAME and COMMIT_HASH from environment.
    Query parameters:
    - path: File path within the repository
    """
    file_path = request.args.get("path")

    if not file_path:
        return {"error": "Missing 'path' parameter"}, 400

    # Use environment variables for repo and commit
    repo_name = REPO_NAME
    commit_hash = COMMIT_HASH

    # Sanitize repo name for filesystem (replace / with _)
    sanitized_repo_name = repo_name.replace("/", "_")

    # Build the full path to the file
    data_dir = (
        Path(__file__).parent / "data" / "repos" / sanitized_repo_name / commit_hash
    )
    full_file_path = data_dir / file_path

    # Security check: ensure the file path is within the expected directory
    try:
        full_file_path = full_file_path.resolve()
        data_dir = data_dir.resolve()
        if not str(full_file_path).startswith(str(data_dir)):
            return {"error": "Invalid file path"}, 400
    except Exception:
        return {"error": "Invalid file path"}, 400

    # Check if file exists
    if not full_file_path.exists() or not full_file_path.is_file():
        return {"error": f"File not found: {file_path}"}, 404

    try:
        # Read file content
        with open(full_file_path, "r", encoding="utf-8") as f:
            content = f.read()

        return {
            "repo": repo_name,
            "commit": commit_hash,
            "path": file_path,
            "content": content,
        }, 200
    except UnicodeDecodeError:
        # Handle binary files or files with encoding issues
        return {"error": "File is not text-readable or has encoding issues"}, 400
    except Exception as e:
        return {"error": f"Error reading file: {str(e)}"}, 500


if __name__ == "__main__":
    # Apple took default port 5000 -- use this port instead
    app.run(debug=True, port=3001)
