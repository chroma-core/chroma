# Chroma Server Middlewares

## Basic Auth

This is very rudimentary security middleware that checks Authorization headers for basic auth credentials.

The basic auth user and pass are configured on the server side using `CHROMA_SERVER_AUTH_PROVIDER_CONFIG`. Make sure to
also define the auth provider `CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.BasicAuthServerProvider"`.

### Usage

Start the server:

```bash
CHROMA_SERVER_AUTH_PROVIDER="chromadb.BasicAuthServerProvider" \
CHROMA_SERVER_AUTH_PROVIDER_CONFIG='{"username":"admin","password":"admin"}' \
ALLOW_RESET=1 \
IS_PERSISTENT=1 \
uvicorn chromadb.app:app --workers 1 --host 0.0.0.0 --port 8000  --proxy-headers --log-config log_config.yml --reload
```

Test request with `curl`:

```bash
curl http://localhost:8000/api/v1 -v -H "Authorization: Basic YWRtaW46YWRtaW4="
```

Test with client side auth provider:

```python
import chromadb
from chromadb import Settings

client = chromadb.HttpClient(settings=Settings(chroma_client_auth_provider="chromadb.auth.BasicAuthClientProvider",
                                               chroma_client_auth_provider_config={"username": "admin", "password": "admin"}))
client.heartbeat()
```

Test with Http Client and basic auth header:

```python
import chromadb

client = chromadb.HttpClient(host="localhost", port="8000", headers={"Authorization": "Basic YWRtaW46YWRtaW4="})
client.heartbeat()
```
