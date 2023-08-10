# Chroma Server Middlewares


## Simple Token Auth Middleware

This is very rudimentary security middleware that checks Authorization headers for a static token.

The static token is configure on the server side using `CHROMA_SERVER_MIDDLEWARE_TOKEN_AUTH_TOKEN`

### Usage

Start the server:

```bash
ALLOW_RESET=1 \
IS_PERSISTENT=1 \
CHROMA_SERVER_MIDDLEWARES='["chromadb.server.middlewares.SimpleTokenAuthMiddleware"]' \
CHROMA_SERVER_MIDDLEWARE_TOKEN_AUTH_ENABLED=true \
CHROMA_SERVER_MIDDLEWARE_TOKEN_AUTH_TOKEN=test \
uvicorn chromadb.app:app --workers 1 --host 0.0.0.0 --port 8000  \
--proxy-headers --log-config log_config.yml --reload
```

Test request with `curl`:

```bash
curl http://localhost:8000/api/v1 -v -H "Authorization: Token test"
```

> **Note**: The authorization header should contain at least two parts Token + the actual token

Test with http client:

```python
import chromadb

client = chromadb.HttpClient(host="localhost", port="8000", headers={"Authorization": "Token test"})
client.heartbeat()
```
