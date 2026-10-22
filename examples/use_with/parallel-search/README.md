# Save web research in Chroma with Parallel Search MCP

Search the web or fetch a known page, save its excerpts with source URLs in a
local Chroma collection, then retrieve relevant excerpts later. This example uses
[Parallel Search MCP](https://docs.parallel.ai/integrations/mcp/search-mcp) over
Streamable HTTP. Search uses Fast mode. No account or API key is needed; the free
endpoint has rate limits and is intended for light use.

## Setup

Use Python 3.10 or later. From this directory:

```bash
python -m venv .venv
source .venv/bin/activate # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Chroma's default embedding function runs locally. Its first use downloads the
ONNX embedding model, so allow internet access and disk space for that download.
There is no language model or generated answer in this example.

## Search and save excerpts

```bash
python web_research.py search "How does Chroma filter documents by metadata?" \
  --search-query "Chroma metadata filtering documentation" \
  --search-query "Chroma where filter operators"
```

This calls `web_search` and stores nonempty excerpts in the `web_research`
collection. Long excerpts are split into small chunks. Each chunk keeps the
page's title and URL. Content-based IDs make repeated ingestion of the same
excerpts idempotent; changed excerpts are retained alongside earlier ones.

## Fetch a specific page

When you need a particular page rather than search results:

```bash
python web_research.py fetch https://docs.trychroma.com/docs/querying-collections/metadata-filtering \
  --objective "Find examples of Chroma metadata filters"
```

This calls `web_fetch` and saves its excerpts in the same collection. Failed
pages are reported; successful pages in a partial response are still saved.
Search queries, fetch URLs, and objectives are sent to Parallel. Stored excerpts
are retrieved snapshots, not full pages or continuously refreshed content.

## Retrieve saved research

```bash
python web_research.py query "How can I filter a collection by metadata?"
```

Retrieval uses local Chroma embeddings and prints matching excerpts with source
URLs. It makes no MCP calls. Data persists in `.chroma-parallel` by default;
use `--path /path/to/research` **before** the subcommand to choose another store.
Keep the same path for ingestion and retrieval. Delete that directory when you
want to discard the example's saved data.
