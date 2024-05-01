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
