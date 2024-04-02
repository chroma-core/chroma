# Authorization

## Configuration

### Resource Actions

```yaml
resource_type_action: # This is here just for reference
  - tenant:create_tenant
  - tenant:get_tenant
  - db:create_database
  - db:get_database
  - db:reset
  - db:list_collections
  - collection:get_collection
  - db:create_collection
  - db:get_or_create_collection
  - collection:delete_collection
  - collection:update_collection
  - collection:add
  - collection:delete
  - collection:get
  - collection:query
  - collection:peek #from API perspective this is the same as collection:get
  - collection:count
  - collection:update
  - collection:upsert
```

### Role Mapping

Following are the role mappings where we define roles and the actions they can perform. The actions spaces is taken from the resource actions defined above.

> **Note**: We also plan to support resource level authorization soon but for now only RBAC is available.

```yaml
roles_mapping:
  admin:
    actions:
      [
        db:list_collections,
        collection:get_collection,
        db:create_collection,
        db:get_or_create_collection,
        collection:delete_collection,
        collection:update_collection,
        collection:add,
        collection:delete,
        collection:get,
        collection:query,
        collection:peek,
        collection:update,
        collection:upsert,
        collection:count,
      ]
  write:
    actions:
      [
        db:list_collections,
        collection:get_collection,
        db:create_collection,
        db:get_or_create_collection,
        collection:delete_collection,
        collection:update_collection,
        collection:add,
        collection:delete,
        collection:get,
        collection:query,
        collection:peek,
        collection:update,
        collection:upsert,
        collection:count,
      ]
  db_read:
    actions:
      [
        db:list_collections,
        collection:get_collection,
        db:create_collection,
        db:get_or_create_collection,
        collection:delete_collection,
        collection:update_collection,
      ]
  collection_read:
    actions:
      [
        db:list_collections,
        collection:get_collection,
        collection:get,
        collection:query,
        collection:peek,
        collection:count,
      ]
  collection_x_read:
    actions:
      [
        collection:get_collection,
        collection:get,
        collection:query,
        collection:peek,
        collection:count,
      ]
    resources: ["<UUID>"] #not yet supported
```

You can update the roll mapping as per your requirements.

### Users

Last piece of the puzzle is the user configuration. Here we define the user id, role and the tokens they can use to authenticate.

> **Note**: In our example we use both AuthN and AuthZ where AuthN verifies whether a token is valid e.g. user has that token and AuthZ verifies whether the user has the right role to perform the action.

```yaml
users:
  - id: user@example.com
    role: admin
    tokens:
      - token: test-token-admin
        secret: my_api_secret # not yet supported
  - id: Anonymous
    role: admin
    tokens:
      - token: my_api_token
        secret: my_api_secret
```

## Starting the Server

```bash
IS_PERSISTENT=1 \
CHROMA_SERVER_AUTHZ_PROVIDER="chromadb.auth.authz.SimpleRBACAuthorizationProvider" \
CHROMA_SERVER_AUTH_CREDENTIALS_FILE=examples/basic_functionality/authz/authz.yaml \
CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER="user_token_config" \
CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.token.TokenAuthenticationServerProvider" \
CHROMA_SERVER_AUTHZ_CONFIG_FILE=examples/basic_functionality/authz/authz.yaml \
uvicorn chromadb.app:app --workers 1 --host 0.0.0.0 --port 8000 --proxy-headers --log-config chromadb/log_config.yml --reload --timeout-keep-alive 30
```

## Testing the authorization

```python
import chromadb
from chromadb.config import Settings

client = chromadb.HttpClient("http://localhost:8000/",
                             settings=Settings(chroma_client_auth_provider="chromadb.auth.token.TokenAuthClientProvider",
                                               chroma_client_auth_credentials="test-token-admin"))

client.list_collections()
collection = client.get_or_create_collection("test_collection")

collection.add(documents=["test"],ids=["1"])
collection.get()
```
