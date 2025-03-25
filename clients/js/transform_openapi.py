#!/usr/bin/env python3
import json
import sys
from typing import Any


def transform_schema(schema: dict[Any, Any] | list[Any]) -> dict[Any, Any] | list[Any]:
    if isinstance(schema, dict):
        # Handle empty schemas
        if not schema:
            schema["type"] = "object"

        # Handle schemas with only default: null
        if len(schema) == 1 and "default" in schema and schema["default"] is None:
            schema["type"] = "object"
            schema["nullable"] = True

        # Handle oneOf with null type
        if "oneOf" in schema and isinstance(schema["oneOf"], list):
            if len(schema["oneOf"]) == 2 and schema["oneOf"][0].get("type") == "null":
                schema["nullable"] = True
                schema["oneOf"] = [schema["oneOf"][1]]

        # Handle array type with null
        if "type" in schema and isinstance(schema["type"], list):
            if len(schema["type"]) == 2 and "null" in schema["type"]:
                schema["nullable"] = True
                schema["type"] = next(t for t in schema["type"] if t != "null")

        # Handle empty items in arrays
        if "items" in schema and not schema["items"]:
            schema["items"] = {"type": "object"}

        # Recursively transform nested objects
        for key, value in schema.items():
            if isinstance(value, (dict, list)):
                schema[key] = transform_schema(value)

    elif isinstance(schema, list):
        return [transform_schema(item) for item in schema]

    return schema


def main() -> None:
    # Read the OpenAPI schema from stdin
    schema = json.load(sys.stdin)

    # Transform the schema
    transformed_schema = transform_schema(schema)

    # Write the transformed schema to stdout
    json.dump(transformed_schema, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
