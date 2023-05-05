## Demo in browser

Update your settings to add `localhost:3000` to `chroma_server_cors_allow_origins`.

For example:

```
client = chromadb.Client(
    Settings(chroma_api_impl="rest", chroma_server_host="localhost", chroma_server_http_port="8000", chroma_server_cors_allow_origins=["http://localhost:3000"])
)

```

1. `yarn dev`
2. visit `localhost:3000`
