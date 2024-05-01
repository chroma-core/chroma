---
title: "✈️ Migration"
---

Schema and data format changes are a necessary evil of evolving software. We take changes seriously and make them infrequently and only when necessary.

Chroma's commitment is whenever schema or data format change, we will provide a seamless and easy-to-use migration tool to move to the new schema/format.

Specifically we will announce schema changes on:

- Discord ([#migrations channel](https://discord.com/channels/1073293645303795742/1129286514845691975))
- Github (here)
- Email listserv [Sign up](https://airtable.com/shrHaErIs1j9F97BE)

We will aim to provide:

- a description of the change and the rationale for the change.
- a CLI migration tool you can run
- a video walkthrough of using the tool

## Migration Log

### Auth overhaul - April 20, 2024

**If you are not using Chroma's [built-in auth system](https://docs.trychroma.com/usage-guide#authentication), you do not need to take any action.**

This release overhauls and simplifies our authentication and authorization systems.
If you are you using Chroma's built-in auth system, you will need to update your configuration and
any code you wrote to implement your own authentication or authorization providers.
This change is mostly to pay down some of Chroma's technical debt and make future changes easier,
but it also changes and simplifies user configuration.
If you are not using Chroma's built-in auth system, you do not need to take any action.

Previously, Chroma's authentication and authorization relied on many objects with many configuration options, including:

- `chroma_server_auth_provider`
- `chroma_server_auth_configuration_provider`
- `chroma_server_auth_credentials_provider`
- `chroma_client_auth_credentials_provider`
- `chroma_client_auth_protocol_adapter`

and others.

We have consolidated these into three classes:

- `ClientAuthProvider`
- `ServerAuthenticationProvider`
- `ServerAuthorizationProvider`

`ClientAuthProvider`s are now responsible for their own configuration and credential management. Credentials can be given to them with the `chroma_client_auth_credentials` setting. The value for `chroma_client_auth_credentials` depends on the `ServerAuthenticationProvider`; for `TokenAuthenticationServerProvider` it should just be the token, and for `BasicAuthenticationServerProvider` it should be `username:password`.

`ServerAuthenticationProvider`s are responsible for turning a request's authorization information into a `UserIdentity` containing any information necessary to make an authorization decision. They are now responsible for their own configuration and credential management. Configured via the `chroma_server_authn_credentials` and `chroma_server_authn_credentials_file` settings.

`ServerAuthorizationProvider`s are responsible for turning information about the request and the `UserIdentity` which issued the request into an authorization decision. Configured via the `chroma_server_authz_config` and `chroma_server_authz_config_file` settings.

*Either `_authn_credentials` or `authn_credentials_file` can be set, never both. Same for `authz_config` and `authz_config_file`. The value of the config (or data in the config file) will depend on your authn and authz providers. See [here](https://github.com/chroma-core/chroma/tree/main/examples/basic_functionality/authz) for more information.*

The two auth systems Chroma ships with are `Basic` and `Token`. We have a small migration guide for each.

#### Basic

If you're using `Token` auth, your server configuration might look like:

```yaml
CHROMA_SERVER_AUTH_CREDENTIALS="admin:admin"
CHROMA_SERVER_AUTH_CREDENTIALS_FILE="./example_file"
CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER="chromadb.auth.providers.HtpasswdConfigurationServerAuthCredentialsProvider"
CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.basic.BasicAuthServerProvider"
```

*Note: Only one of `AUTH_CREDENTIALS` and `AUTH_CREDENTIALS_FILE` can be set, but this guide shows how to migrate both.*

And your corresponding client configation:

```yaml
CHROMA_CLIENT_AUTH_PROVIDER="chromadb.auth.token.TokenAuthClientProvider"
CHROMA_CLIENT_AUTH_CREDENTIALS="admin:admin"
```

To migrate to the new server configuration, simply change it to:

```yaml
CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
CHROMA_SERVER_AUTHN_CREDENTIALS="test-token"
CHROMA_SERVER_AUTHN_CREDENTIALS_FILE="./example_file"
```

New client configuration:

```yaml
CHROMA_CLIENT_AUTH_CREDENTIALS="test-token"
CHROMA_CLIENT_AUTH_PROVIDER="chromadb.auth.basic_authn.BasicAuthClientProvider"
```

#### Token

If you're using `Token` auth, your server configuration might look like:

```yaml
CHROMA_SERVER_AUTH_CREDENTIALS="test-token"
CHROMA_SERVER_AUTH_CREDENTIALS_FILE="./example_file"
CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER="chromadb.auth.token.TokenConfigServerAuthCredentialsProvider"
CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.token.TokenAuthServerProvider"
CHROMA_SERVER_AUTH_TOKEN_TRANSPORT_HEADER="AUTHORIZATION"
```

*Note: Only one of `AUTH_CREDENTIALS` and `AUTH_CREDENTIALS_FILE` can be set, but this guide shows how to migrate both.*

And your corresponding client configation:

```yaml
CHROMA_CLIENT_AUTH_PROVIDER="chromadb.auth.token.TokenAuthClientProvider"
CHROMA_CLIENT_AUTH_CREDENTIALS="test-token"
CHROMA_CLIENT_AUTH_TOKEN_TRANSPORT_HEADER="AUTHORIZATION"
```

To migrate to the new server configuration, simply change it to:

```yaml
CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
CHROMA_SERVER_AUTHN_CREDENTIALS="test-token"
CHROMA_SERVER_AUTHN_CREDENTIALS_FILE="./example_file"
CHROMA_AUTH_TOKEN_TRANSPORT_HEADER="AUTHORIZATION"
```

New client configuration:

```yaml
CHROMA_CLIENT_AUTH_CREDENTIALS="test-token"
CHROMA_CLIENT_AUTH_PROVIDER="chromadb.auth.token_authn.TokenAuthClientProvider"
CHROMA_AUTH_TOKEN_TRANSPORT_HEADER="AUTHORIZATION"
```

#### Reference of changed configuration values

- Overall config
  - `chroma_client_auth_token_transport_header`: renamed to `chroma_auth_token_transport_header`.
  - `chroma_server_auth_token_transport_header`: renamed to `chroma_auth_token_transport_header`.
- Client config
  - `chroma_client_auth_credentials_provider`: deleted. Functionality is now in `chroma_client_auth_provider`.
  - `chroma_client_auth_protocol_adapter`: deleted. Functionality is now in `chroma_client_auth_provider`.
  - `chroma_client_auth_credentials_file`: deleted. Functionality is now in `chroma_client_auth_credentials`.
  - These changes also apply to the Typescript client.
- Server authn
  - `chroma_server_auth_provider`: Renamed to `chroma_server_authn_provider`.
  - `chroma_server_auth_configuration_provider`: deleted. Functionality is now in `chroma_server_authn_provider`.
  - `chroma_server_auth_credentials_provider`: deleted. Functionality is now in `chroma_server_authn_provider`.
  - `chroma_server_auth_credentials_file`: renamed to `chroma_server_authn_credentials_file`.
  - `chroma_server_auth_credentials`: renamed to `chroma_server_authn_credentials`.
  - `chroma_server_auth_configuration_file`: renamed to `chroma_server_authn_configuration_file`.
- Server authz
  - `chroma_server_authz_ignore_paths`: deleted. Functionality is now in `chroma_server_auth_ignore_paths`.

To see the full changes, you can read the [PR](https://github.com/chroma-core/chroma/pull/1970/files) or reach out to the Chroma team on [Discord](https://discord.gg/MMeYNTmh3x).

### Migration to 0.4.16 - November 7, 2023

This release adds support for multi-modal embeddings, with an accompanying change to the definitions of `EmbeddingFunction`.
This change mainly affects users who have implemented their own `EmbeddingFunction` classes. If you are using Chroma's built-in embedding functions, you do not need to take any action.

**EmbeddingFunction**

Previously, `EmbeddingFunction`s were defined as:

```python
class EmbeddingFunction(Protocol):
    def __call__(self, texts: Documents) -> Embeddings:
        ...
```

After this update, `EmbeddingFunction`s are defined as:

```python
Embeddable = Union[Documents, Images]
D = TypeVar("D", bound=Embeddable, contravariant=True)

class EmbeddingFunction(Protocol[D]):
    def __call__(self, input: D) -> Embeddings:
        ...
```

The key differences are:
- `EmbeddingFunction` is now generic, and takes a type parameter `D` which is a subtype of `Embeddable`. This allows us to define `EmbeddingFunction`s which can embed multiple modalities.
- `__call__` now takes a single argument, `input`, to support data of any type `D`. The `texts` argument has been removed.



### Migration from >0.4.0 to 0.4.0 - July 17, 2023

What's new in this version?
- New easy way to create clients
- Changed storage method
- `.persist()` removed, `.reset()` no longer on by default

**New Clients**

```python
### in-memory ephemeral client

# before
import chromadb
client = chromadb.Client()

# after
import chromadb
client = chromadb.EphemeralClient()


### persistent client

# before
import chromadb
from chromadb.config import Settings
client = chromadb.Client(Settings(
    chroma_db_impl="duckdb+parquet",
    persist_directory="/path/to/persist/directory" # Optional, defaults to .chromadb/ in the current directory
))

# after
import chromadb
client = chromadb.PersistentClient(path="/path/to/persist/directory")


### http client (to talk to server backend)

# before
import chromadb
from chromadb.config import Settings
client = chromadb.Client(Settings(chroma_api_impl="rest",
                                        chroma_server_host="localhost",
                                        chroma_server_http_port="8000"
                                    ))

# after
import chromadb
client = chromadb.HttpClient(host="localhost", port="8000")

```

You can still also access the underlying `.Client()` method. If you want to turn off telemetry, all clients support custom settings:

```python
import chromadb
from chromadb.config import Settings
client = chromadb.PersistentClient(
    path="/path/to/persist/directory",
    settings=Settings(anonymized_telemetry=False))
```

**New data layout**

This version of Chroma drops `duckdb` and `clickhouse` in favor of `sqlite` for metadata storage. This means migrating data over. We have created a migration CLI utility to do this.

If you upgrade to `0.4.0` and try to access data stored in the old way, you will see this error message


> You are using a deprecated configuration of Chroma. Please pip install chroma-migrate and run `chroma-migrate` to upgrade your configuration. See https://docs.trychroma.com/migration for more information or join our discord at https://discord.gg/8g5FESbj for help!

Here is how to install and use the CLI:

```
pip install chroma-migrate
chroma-migrate
```

![](/img/chroma-migrate.png)

If you need any help with this migration, please reach out! We are on [Discord](https://discord.com/channels/1073293645303795742/1129286514845691975) ready to help.

**Persist & Reset**

`.persist()` was in the old version of Chroma because writes were only flushed when forced to. Chroma `0.4.0` saves all writes to disk instantly and so `persist` is no longer needed.

`.reset()`, which resets the entire database, used to by enabled-by-default which felt wrong. `0.4.0` has it disabled-by-default. You can enable it again by passing `allow_reset=True` to a Settings object. For example:

```python
import chromadb
from chromadb.config import Settings
client = chromadb.PersistentClient(path="./path/to/chroma", settings=Settings(allow_reset=True))
```
