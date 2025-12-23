#!/usr/bin/env python3

import json
import sys


def ensure_hashmap_schema(schemas):
    if "HashMap" in schemas:
        schema = schemas["HashMap"]
        schema.setdefault("type", "object")
        schema.setdefault("additionalProperties", {})


def ensure_sparse_vector_schema(schemas):
    sparse = schemas.get("SparseVector")
    if not sparse:
        return
    props = sparse.setdefault("properties", {})
    if "tokens" not in props:
        props["tokens"] = {"type": "array", "items": {"type": "string"}, "nullable": True}


def mark_metadata_nullable(schemas):
    for schema in schemas.values():
        props = schema.get("properties", {})
        for key, value in props.items():
            if key == "metadata":
                value.setdefault("nullable", True)


def main(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    schemas = data.get("components", {}).get("schemas", {})
    if schemas:
        ensure_hashmap_schema(schemas)
        ensure_sparse_vector_schema(schemas)
        mark_metadata_nullable(schemas)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: patch_openapi.py <openapi.json>")
    main(sys.argv[1])
