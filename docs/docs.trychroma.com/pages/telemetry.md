---
title: "📏 Telemetry"
---

Chroma contains a telemetry feature that collects **anonymous** usage information.

## Why?

We use this information to help us understand how Chroma is used, to help us prioritize work on new features and bug fixes, and to help us improve Chroma’s performance and stability.

## Opting out

If you prefer to opt out of telemetry, you can do this in two ways.

###### In Client Code

Set `anonymized_telemetry` to `False` in your client's settings:

```python
from chromadb.config import Settings
client = chromadb.Client(Settings(anonymized_telemetry=False))
# or if using PersistentClient
client = chromadb.PersistentClient(path="/path/to/save/to", settings=Settings(anonymized_telemetry=False))
```

###### In Chroma's Backend Using Environment Variables

Set `ANONYMIZED_TELEMETRY` to `False` in your shell or server environment.

If you are running Chroma on your local computer with `docker-compose` you can set this value in an `.env` file placed in the same directory as the `docker-compose.yml` file:

```
ANONYMIZED_TELEMETRY=False
```

## What do you track?

We will only track usage details that help us make product decisions, specifically:

- Chroma version and environment details (e.g. OS, Python version, is it running in a container, or in a jupyter notebook)
- Usage of embedding functions that ship with Chroma and aggregated usage of custom embeddings (we collect no information about the custom embeddings themselves)
- Collection commands. We track the anonymized uuid of a collection as well as the number of items
  - `add`
  - `update`
  - `query`
  - `get`
  - `delete`

We **do not** collect personally-identifiable or sensitive information, such as: usernames, hostnames, file names, environment variables, or hostnames of systems being tested.

To view the list of events we track, you may reference the **[code](https://github.com/chroma-core/chroma/blob/main/chromadb/telemetry/product/events.py)**

## Where is telemetry information stored?

We use **[Posthog](https://posthog.com/)** to store and visualize telemetry data.

> Posthog is an open source platform for product analytics. Learn more about Posthog on **[posthog.com](https://posthog.com/)** or **[github.com/posthog](https://github.com/posthog/posthog)**
