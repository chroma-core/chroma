#!/usr/bin/env python3

# This script is used to transform the OpenAPI spec to ensure that the null type and HashMap is handled correctly.
import json
import sys
import urllib.request
from urllib.error import URLError
from typing import Any


def fetch_openapi_json(url: str) -> Any:
    """Fetch OpenAPI JSON from a URL"""
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read().decode("utf-8")
            return json.loads(data)
    except URLError as e:
        print(f"Error fetching OpenAPI spec: {e}")
        sys.exit(1)
    except json.JSONDecodeError:
        print("Failed to parse JSON from server response")
        sys.exit(1)


def transform_null_oneof(obj: dict[str, Any]) -> None:
    """Transform oneOf patterns with null to nullable references"""
    if obj is None or not isinstance(obj, (dict, list)):
        return

    if isinstance(obj, dict):
        keys_to_process = list(
            obj.keys()
        )  # Create a copy of keys to avoid modification during iteration

        for key in keys_to_process:
            value = obj[key]

            # Handle explicit null type
            if key == "type" and value == "null":
                # Replace with nullable true on parent and remove this key
                parent_keys = [k for k in obj.keys() if k != "type"]
                if len(parent_keys) > 0:
                    obj["nullable"] = True
                    del obj["type"]
                    print("Transformed direct null type to nullable=true")
                continue

            if isinstance(value, dict):
                # Check if this is a oneOf with a null type option
                if "oneOf" in value and isinstance(value["oneOf"], list):
                    null_schemas = [
                        s for s in value["oneOf"] if s.get("type") == "null"
                    ]
                    other_schemas = [
                        s
                        for s in value["oneOf"]
                        if s.get("type") != "null" or "type" not in s
                    ]

                    # If we found oneOf with any null schemas, process them
                    if null_schemas:
                        # If only one other schema exists, make it nullable
                        if len(other_schemas) == 1:
                            other_schema = other_schemas[0]
                            del obj[key]["oneOf"]

                            # If the other schema is a reference, keep it and add nullable
                            if "$ref" in other_schema:
                                obj[key]["$ref"] = other_schema["$ref"]
                                obj[key]["nullable"] = True
                                print(
                                    "Transformed oneOf with $ref to nullable reference"
                                )
                            # If the other schema is not a reference, copy its properties and add nullable
                            else:
                                obj[key].update(other_schema)
                                obj[key]["nullable"] = True
                                print("Transformed oneOf to nullable schema properties")
                        else:
                            # For multiple other schemas, we'll preserve oneOf but remove null schemas
                            obj[key]["oneOf"] = other_schemas
                            obj[key]["nullable"] = True
                            print(
                                "Transformed oneOf with multiple schemas to nullable=true"
                            )

                # If it's a property of type array with items containing oneOf
                elif "type" in value and value["type"] == "array" and "items" in value:
                    # Handle array items
                    if isinstance(value["items"], dict):
                        transform_null_oneof(value["items"])
                    elif isinstance(value["items"], list):
                        for item in value["items"]:
                            transform_null_oneof(item)

                # Regular recursive processing
                transform_null_oneof(value)

            elif isinstance(value, list):
                for item in value:
                    transform_null_oneof(item)

    # If obj is a list, process each item
    elif isinstance(obj, list):
        for item in obj:
            transform_null_oneof(item)


def process_schema_references(obj: dict[str, Any]) -> None:
    """Process schema references to ensure they don't contain null types"""
    if isinstance(obj, dict):
        # If this is a schema definition
        schemas = obj.get("components", {}).get("schemas", {})
        for schema_name, schema in schemas.items():
            print(f"Processing schema: {schema_name}")
            transform_null_oneof(schema)


def modify_reset_endpoint_response(openapi_json: dict[str, Any]) -> None:
    """Modify the /api/v2/reset endpoint 200 response to use application/json."""
    try:
        reset_path = (
            openapi_json.get("paths", {}).get("/api/v2/reset", {}).get("post", {})
        )
        responses = reset_path.get("responses", {})
        response_200 = responses.get("200", {})
        content = response_200.get("content", {})

        if "text/plain" in content and "schema" in content["text/plain"]:
            print(
                "Modifying /api/v2/reset 200 response from text/plain to application/json"
            )
            boolean_schema = content["text/plain"]["schema"]
            del content["text/plain"]
            content["application/json"] = {"schema": boolean_schema}
        else:
            print(
                "Could not find text/plain schema in /api/v2/reset 200 response to modify."
            )

    except Exception as e:
        print(f"Error modifying reset endpoint: {e}")


def modify_version_endpoint_response(openapi_json: dict[str, Any]) -> None:
    """Modify the /api/v2/version endpoint 200 response to use application/json."""
    try:
        # Assuming GET method for version endpoint
        version_path = (
            openapi_json.get("paths", {}).get("/api/v2/version", {}).get("get", {})
        )
        responses = version_path.get("responses", {})
        response_200 = responses.get("200", {})
        content = response_200.get("content", {})

        if "text/plain" in content and "schema" in content["text/plain"]:
            print(
                "Modifying /api/v2/version 200 response from text/plain to application/json"
            )
            version_schema = content["text/plain"]["schema"]
            del content["text/plain"]
            content["application/json"] = {"schema": version_schema}
        else:
            print(
                "Could not find text/plain schema in /api/v2/version 200 response to modify."
            )

    except Exception as e:
        print(f"Error modifying version endpoint: {e}")


def main() -> None:
    url = "http://localhost:8000/openapi.json"
    output_file = "openapi.json"

    print(f"Fetching OpenAPI spec from {url}")
    openapi_json = fetch_openapi_json(url)

    print("Processing schema definitions first...")
    process_schema_references(openapi_json)

    # Add the new modification step
    print("Modifying specific endpoint responses...")
    modify_reset_endpoint_response(openapi_json)
    modify_version_endpoint_response(openapi_json)

    print(f"Writing transformed OpenAPI spec to {output_file}")
    with open(output_file, "w") as f:
        json.dump(openapi_json, f, indent=2)

    print("OpenAPI specification transformed successfully!")


if __name__ == "__main__":
    main()
