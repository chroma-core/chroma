#!/usr/bin/env python3
"""Talk to transactional Chroma with the Python client.

Run against a Chroma server that supports conditional transactions:

    python3 examples/conditional_transactions.py

Optional environment:

    CHROMA_HOST=localhost
    CHROMA_PORT=8000
    CHROMA_SSL=false
    CHROMA_TENANT=default_tenant
    CHROMA_DATABASE=default_database
    CHROMA_API_KEY=...
"""

from __future__ import annotations

import os
from typing import Any, Optional

import chromadb


COLLECTION_NAME = "transactional_chroma_python_example"
RECORD_ID = "txn-doc"
EMBEDDING = [1.0, 0.0, 0.0]


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def headers_from_env() -> Optional[dict[str, str]]:
    api_key = os.environ.get("CHROMA_API_KEY")
    if api_key is None:
        return None
    return {"x-chroma-token": api_key}


def main() -> None:
    client = chromadb.HttpClient(
        host=os.environ.get("CHROMA_HOST", "localhost"),
        port=int(os.environ.get("CHROMA_PORT", "8000")),
        ssl=env_bool("CHROMA_SSL"),
        headers=headers_from_env(),
        tenant=os.environ.get("CHROMA_TENANT", "default_tenant"),
        database=os.environ.get("CHROMA_DATABASE", "default_database"),
    )

    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass

    collection = client.create_collection(
        name=COLLECTION_NAME,
        embedding_function=None,
    )

    def create_or_update(txn: Any) -> str:
        existing = txn.get(ids=RECORD_ID, include=["metadatas"])
        if existing["ids"]:
            txn.update(
                ids=RECORD_ID,
                metadatas={"status": "updated-by-run", "version": 1},
            )
            return "updated"

        txn.add(
            ids=RECORD_ID,
            embeddings=EMBEDDING,
            metadatas={"status": "created-by-run", "version": 1},
        )
        return "created"

    outcome = collection.conditional().run(create_or_update, max_retries=3)
    print(f"run() transaction {outcome} {RECORD_ID!r}")

    txn = collection.conditional()
    before = txn.get(ids=RECORD_ID, include=["metadatas"])
    if not before["ids"]:
        raise RuntimeError(f"{RECORD_ID!r} disappeared before manual commit")
    txn.update(
        ids=RECORD_ID,
        metadatas={"status": "updated-by-manual-commit", "version": 2},
    )
    committed = txn.commit()
    print(f"manual commit wrote {committed['record_count']} record(s)")

    after = collection.get(ids=RECORD_ID, include=["metadatas", "embeddings"])
    print(after)


if __name__ == "__main__":
    main()
