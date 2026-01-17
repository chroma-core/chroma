#!/usr/bin/env python3
"""
Cross-language embedding function persistence test harness.
Creates collections with specific EFs in Python for Go client validation.
Supports both self-hosted and Chroma Cloud deployments.
"""
import argparse
import json
import sys
import os
from urllib.parse import urlparse

from dotenv import load_dotenv
import chromadb

load_dotenv()

SAMPLE_DOCUMENTS = [
    "The quick brown fox jumps over the lazy dog",
    "Machine learning models process natural language",
    "Vector databases enable semantic search applications",
    "Python and Go are popular programming languages",
    "Embeddings represent text as numerical vectors",
]


def create_default_ef_collection(client, collection_name: str) -> dict:
    """Create collection with default (ONNX) embedding function."""
    from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

    ef = DefaultEmbeddingFunction()

    collection = client.create_collection(name=collection_name, embedding_function=ef)

    ids = [f"doc_{i}" for i in range(len(SAMPLE_DOCUMENTS))]
    collection.add(ids=ids, documents=SAMPLE_DOCUMENTS)

    query_text = "semantic search"
    results = collection.query(query_texts=[query_text], n_results=2)

    return {
        "collection_name": collection_name,
        "ef_type": "default",
        "ef_name": "default",
        "document_count": len(SAMPLE_DOCUMENTS),
        "ids": ids,
        "documents": SAMPLE_DOCUMENTS,
        "verification": {
            "query_text": query_text,
            "expected_ids": results["ids"][0],
            "n_results": 2,
        },
    }


def create_openai_ef_collection(client, collection_name: str) -> dict:
    """Create collection with OpenAI embedding function."""
    from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

    # Python chromadb expects CHROMA_OPENAI_API_KEY, but we also accept OPENAI_API_KEY
    api_key = os.environ.get("CHROMA_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("CHROMA_OPENAI_API_KEY or OPENAI_API_KEY environment variable required")

    # Set CHROMA_OPENAI_API_KEY so chromadb stores the correct env var name
    os.environ["CHROMA_OPENAI_API_KEY"] = api_key

    ef = OpenAIEmbeddingFunction(model_name="text-embedding-3-small")

    collection = client.create_collection(name=collection_name, embedding_function=ef)

    ids = [f"doc_{i}" for i in range(len(SAMPLE_DOCUMENTS))]
    collection.add(ids=ids, documents=SAMPLE_DOCUMENTS)

    query_text = "semantic search"
    results = collection.query(query_texts=[query_text], n_results=2)

    return {
        "collection_name": collection_name,
        "ef_type": "openai",
        "ef_name": "openai",
        "document_count": len(SAMPLE_DOCUMENTS),
        "ids": ids,
        "documents": SAMPLE_DOCUMENTS,
        "verification": {
            "query_text": query_text,
            "expected_ids": results["ids"][0],
            "n_results": 2,
        },
        "config": {
            "api_key_env_var": "CHROMA_OPENAI_API_KEY",
            "model_name": "text-embedding-3-small",
        },
    }


def delete_collection_if_exists(client, collection_name: str):
    """Delete collection if it exists."""
    try:
        client.delete_collection(collection_name)
    except Exception:
        pass


def create_client(args):
    """Create Chroma client based on mode (local or cloud)."""
    if args.cloud:
        api_key = args.api_key or os.environ.get("CHROMA_API_KEY")
        tenant = args.tenant or os.environ.get("CHROMA_TENANT")
        database = args.database or os.environ.get("CHROMA_DATABASE")

        if not all([api_key, tenant, database]):
            raise ValueError(
                "Cloud mode requires --api-key, --tenant, --database "
                "or CHROMA_API_KEY, CHROMA_TENANT, CHROMA_DATABASE env vars"
            )

        return chromadb.CloudClient(
            tenant=tenant,
            database=database,
            api_key=api_key,
        )
    else:
        parsed = urlparse(args.endpoint)
        host = parsed.hostname or "localhost"
        port = parsed.port or 8000
        return chromadb.HttpClient(host=host, port=port)


def main():
    parser = argparse.ArgumentParser(description="Cross-language EF test harness")
    parser.add_argument("--endpoint", help="Chroma server endpoint (for local mode)")
    parser.add_argument(
        "--cloud",
        action="store_true",
        help="Use Chroma Cloud instead of local server",
    )
    parser.add_argument("--api-key", help="Chroma Cloud API key")
    parser.add_argument("--tenant", help="Chroma Cloud tenant")
    parser.add_argument("--database", help="Chroma Cloud database")
    parser.add_argument(
        "--ef-type",
        required=True,
        choices=["default", "openai"],
        help="Embedding function type",
    )
    parser.add_argument(
        "--collection-prefix",
        default="crosslang_test_",
        help="Collection name prefix",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete collection before creating (useful for cloud)",
    )
    args = parser.parse_args()

    if not args.cloud and not args.endpoint:
        parser.error("--endpoint is required for local mode")

    client = create_client(args)

    collection_name = f"{args.collection_prefix}{args.ef_type}"

    try:
        if args.cleanup:
            delete_collection_if_exists(client, collection_name)

        if args.ef_type == "default":
            result = create_default_ef_collection(client, collection_name)
        elif args.ef_type == "openai":
            result = create_openai_ef_collection(client, collection_name)

        result["status"] = "success"
        result["mode"] = "cloud" if args.cloud else "local"
        print(json.dumps(result))
        sys.exit(0)
    except Exception as e:
        error_result = {
            "status": "error",
            "error": str(e),
            "collection_name": collection_name,
        }
        print(json.dumps(error_result))
        sys.exit(1)


if __name__ == "__main__":
    main()
