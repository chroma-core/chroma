---
id: overview
name: Overview
---

# Overview

Chroma Sync exposes endpoints for developers to chunk, embed, and index various data sources. The API is intended for Chroma Cloud users and can be accessed for free (up to $5 in credits) by creating a Chroma Cloud account.

# Key Concepts

Chroma Sync has three primary concepts: **source types**, **sources** and **invocations**.

# Source Types

A source type defines a kind of entity that contains data that can be chunked, embedded, and indexed. An example of a source type—and notably the only currently supported one—is a GitHub repository. Each source type defines its own schema for configuring sources of its type. For example, the sources of the GitHub repository type allow developers to define a parameter `include_globs`, which is an array of glob patterns for which matching files will be synced.

Other examples of (future) source types may be S3 buckets, web pages, Notion workspaces, or any other corpus of continually updated content. If there is a specific source type for which you would like support, please reach out to [engineering@trychroma.com](mailto:engineering@trychroma.com).

## GitHub Repositories

The GitHub repository source type allows developers to sync code in public and private GitHub repositories. Public repositories require no setup other than creating a Chroma Cloud account and issuing an API key. Chroma Sync for private repositories is available at two different tiers: direct and platform.

### Direct Sync

The direct tier requires you to install Chroma’s GitHub App into any repository for which you wish to perform syncing. The direct tier is only available via the Chroma Cloud UI and does not enable you to perform Sync-related operations via the API. This tier is ideal for developers who wish to sync private repositories that they own. If you are interested in using the direct tier via API, please reach out to us at [engineering@trychroma.com](mailto:engineering@trychroma.com).

### Platform Sync

The platform tier requires you to grant Chroma access to a GitHub App that you own, which has been installed into the private repositories you wish to sync. This GitHub App must have read-only access to the “Contents” and “Metadata” permissions on the list of “Repository permissions”.

The platform tier grants access to the Chroma Sync API and is ideal for companies and organizations that offer services which access their users’ codebases. For a detailed walkthrough, see [Platform Sync docs](/cloud/sync/github#platform-sync).

## Web

The web source type allows developers to scrape the contents of web pages into Chroma. Given a starting URL, Sync will crawl the page and its links up to a specified depth.

# Sources

A source is a specific instance of a source type configured according to the global and source type-specific configuration schema. The global source configuration schema refers to the configuration parameters that are required across sources of all types, while the source-type specific configuration schema refers to the configuration parameters required for a specific source type.

The global source configuration schema requires the following parameters:

```json
{
  "database_name": "string",
  "embedding": {
    "dense": {
        "model": "Qwen/Qwen3-Embedding-0.6B"
    }
  }
}
```

- `database_name` defines the Chroma database in which collections should be created by invocations run on this source. A database must exist before creating sources that point to it.
- `embedding.dense.model` defines the embedding model that should be used to generate dense embeddings for chunked documents. Currently, only the [Qwen3-Embedding-0.6B](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B) model is supported, but if there is a model you would like to use, please let us know by reaching out to [engineering@trychroma.com](mailto:engineering@trychroma.com).

## GitHub Repositories

A source of the GitHub repository type is an individual GitHub repository configured with the global source configuration parameters, and the GitHub source-specific configuration parameters:

```json
{
	"repository": "string",
	"app_id": "string" | null, // optional
	"include_globs": ["string", ...] | null, // optional
}
```

- `repository` defines the GitHub repository whose code should be synced. This must be the forward slash-separated combination of the repository owner’s GitHub username and the repository name (e.g., `chroma-core/chroma`). Note that changing a repository name after creating a Chroma Sync source for it will result in invocations on that source failing, so a new source with the updated repository name must be created.
- `app_id` defines the GitHub App ID of the GitHub App that has access to the provided `repository`. This parameter should only be supplied if the provided repository is private. If you are unsure of the GitHub App ID you should use, [see more](https://www.notion.so/Chroma-Sync-Docs-28b58a6d81918062b6ebf00deedde0ab?pvs=21) about the two tiers Chroma offers for the GitHub repository source type.
- `include_globs` defines a set of glob patterns for which matching files should be synced. If this parameter is not provided, files matching `"*"` will be synced. Note that Chroma will not sync binary data, images, and other large or non-UTF-8 files.

## Web

A source of the web type is configured with a starting URL and a few other optional parameters:

```json
{
    "starting_url": "https://docs.trychroma.com",
    // all below are optional
    "page_limit": 5,
    "include_path_regexes": ["/cloud/*"],
    "exclude_path_regexes": ["/blog/*"],
    "max_depth": 2
}
```

# Invocations

Invocations refer to runs of the Sync Function over the data in a source. One invocation corresponds to one sync pass through all of the data in a source. A single invocation will result in the creation of exactly one collection in the database specified by the invocation’s source. This collection will contain the chunked, embedded, and indexed data that represents the state of the source at the time of the invocation’s creation. Invocations, like sources, have some global configuration parameters, as well as parameters specific to the type of the source for which the invocation is being run.

The global invocation configuration parameters are:

```json
{
	"target_collection_name": "string"
}
```

- `target_collection_name` defines the name of the Chroma collection in which synced data should be stored. This must be a collection that does not already exist with synced data. Chroma Sync uses the metadata key `finished_ingest` to indicate whether a collection contains synced data. If an invocation creation request is received for a collection with metadata in which this key is present and set to true, the API will return a 409 Conflict.

## GitHub Repositories

Invocations on sources of the GitHub repository type are sync runs over an individual GitHub repository with some set of configuration parameters. The configuration parameters that are specific to invocations on sources of this type are:

```json
{
	"ref_identifier": {
		"$oneOf": {
			"branch": "string",
			"sha": "string"
		}
	}
}
```

- `ref_identifier` is either the commit SHA-256 or the name of the branch from which to retrieve the code to be synced. If a branch is provided, the code will be retrieved from the branch’s latest commit.

# Endpoints

## Sources

### Create Source
Creates a new source of the specified type with the provided configuration.

**Method**

    POST

**Endpoint**

    https://sync.trychroma.com/api/v1/sources

**Required Headers**
- `x-chroma-token` must carry the caller’s Chroma Cloud API key.

**Request Body**

For a GitHub repository source:

```json
{
    "database_name": "string",
    "embedding": {
        "dense": {
            "model": "Qwen/Qwen3-Embedding-0.6B"
        }
    },
    "github": {
        "repository": "string",
        "app_id": "string" | null, // optional
        "include_globs": ["string", ...] | null, // optional
    }
}
```

For a web source:

```json
{
    "database_name": "string",
    "embedding": {
        "dense": {
            "model": "Qwen/Qwen3-Embedding-0.6B"
        }
    },
    "web_scrape": {
        "starting_url": "https://docs.trychroma.com",
        "page_limit": 5
    }
}
```

**Responses**
- `200 OK` If the source is successfully created.

    ```json
    {
        "source_id": "string"
    }
    ```

- `400 Bad Request` If the request payload is missing required fields or a provided field was malformed.
- `401 Unauthorized` If the `x-chroma-token` header is not present or invalid.
- `403 Forbidden` If the user does not have permission to create a source. Note that only team owners and administrators can create sources.
- `404 Not Found` If the provided database or GitHub App does not exist.
- `409 Conflict` If the source already exists.
- `500 Internal Server Error` If an unknown error occurs while creating the source.

### Get Source

Retrieve a specific source by its ID.

**Method**

    GET

**Endpoint**

    https://sync.trychroma.com/api/v1/sources/{source_id}

**Required Headers**
- `x-chroma-token` must carry the caller’s Chroma Cloud API key.

**Request Parameters**
- `source_id` is the ID of the source to retrieve.

**Responses**
- `200 OK` If the source is found.

```json
{
    "id": "string",
    "database_name": "string",
    "embedding": {
        "dense": {
            "model": "string"
        }
    },
    "github": {
        "repository": "string",
        "app_id": "string" | null,
        "include_globs": ["string", ...]
    },
    "created_at": "string"
}
```

- `400 Bad Request` If the request parameters are missing required values or a provided value was malformed.
- `401 Unauthorized` If the `x-chroma-token` header is not present or invalid.
- `403 Forbidden` If the user does not have permission to access this source.
- `404 Not Found` If the requested source does not exist.
- `500 Internal Server Error` If an unknown error occurs while retrieving the source.

### List Sources

List sources with optional filtering.

**Method**

    GET

**Endpoint**

    https://sync.trychroma.com/api/v1/sources

**Required Headers**
- `x-chroma-token` must carry the caller’s Chroma Cloud API key.

**Query Parameters**
- `database_name` allows callers to optionally filter sources by their database.
- `source_type` allows callers to optionally filter sources by their type. If provided, must be one of `[github]`.
- `github.app_id` allows callers to optionally filter GitHub sources by their app ID. If provided, `source_type` must be `github`.
- `github.repository` allows callers to optionally filter GitHub sources by their repository. If provided, `source_type` must be `github`.
- `limit` specifies the maximum number of results to return. Defaults to 100.
- `offset` specifies the number of results to skip before starting to assemble the list of returned sources when sorting by the sources’ `created_at` timestamps. Defaults to 0.
- `order_by` indicates whether to perform sorting by the sources’ `created_at` timestamps in ascending or descending order. If provided, must be one of `[ASC, DESC]`. Defaults to `DESC`.

**Responses**
- `200 OK` If the request does not encounter any errors while listing sources.

    ```json
    [
        {
            "id": "string",
            "database_name": "string",
            "embedding": {
                "dense": {
                    "model": "string"
                }
            },
            "source_type": {
                "github": {
                    "repository": "string",
                    "app_id": "string" | null,
                    "include_globs": ["string", ...]
                }
            },
            "created_at": "string"
        },
        ...
    ]
    ```

- `400 Bad Request` If the request parameters are missing required values or a provided value was malformed.
- `401 Unauthorized` If the `x-chroma-token` header is not present or invalid.
- `403 Forbidden` If the user does not have permission to list sources.
- `500 Internal Server Error` If an unknown error occurs while retrieving sources.

### Delete Source

Delete a source. Does not cancel in-flight invocations on this source.

**Method**

    DELETE

**Endpoint**

    https://sync.trychroma.com/api/v1/sources/{source_id}

**Required Headers**
- `x-chroma-token` must carry the caller’s Chroma Cloud API key.

**Request Parameters**
- `source_id` is the ID of the source to delete.

**Responses**
- `204 No Content` If the source is deleted.
- `400 Bad Request` If the request parameters are missing required values or a provided value was malformed.
- `401 Unauthorized` If the `x-chroma-token` header is not present or invalid.
- `403 Forbidden` If the user does not have permission to delete this source. Note that only team owners and administrators can delete sources.
- `404 Not Found` If the provided source does not exist.
- `500 Internal Server Error` If an unknown error occurs while deleting the source.

## Invocations

### Create Invocation

Creates a new invocation on the specified source with the provided configuration parameters.

**Method**

    POST

**Endpoint**

    https://sync.trychroma.com/api/v1/sources/{source_id}/invocations

**Required Headers**
    - `x-chroma-token` must carry the caller’s Chroma Cloud API key.

**Request Parameters**
- `source_id` is the ID of the source for which to create an invocation.

**Request Body**

```json
{
    "target_collection_name": "string",
    "ref_identifier": {
        "$oneOf": {
            "branch": "string",
            "sha": "string"
        }
    }
}
```

**Responses**
- `200 OK` If the invocation is successfully created.

    ```json
    {
        "invocation_id": "string"
    }
    ```

- `400 Bad Request` If the request payload is missing required fields or a provided field was malformed.
- `401 Unauthorized` If the `x-chroma-token` header is not present or invalid.
- `403 Forbidden` If the user does not have permission to create an invocation.
- `404 Not Found` If the provided source is not found.
- `409 Conflict` If the provided `target_collection_name` exists with `finished_ingest` set to `true`.
- `500 Internal Server Error` If an unknown error occurs while creating the invocation.

### Get Invocation

Retrieve a specific invocation by its ID.

**Method**

    GET

**Endpoint**

    https://sync.trychroma.com/api/v1/invocations/{invocation_id}

**Required Headers**
- `x-chroma-token` must carry the caller’s Chroma Cloud API key.

**Request Parameters**
- `invocation_id` is the ID of the invocation to retrieve.

**Responses**
- `200 OK` If the invocation is found.

    ```json
    {
        "invocation_id": "string",
        "status": {
            "$oneOf": {
                "pending",
                "processing",
                "complete": {
                    "finished_at": "string"
                },
                "failed": {
                    "error": "string"
                },
                "cancelled"
            }
        },
        "source_id": "string",
        "metadata": {
            "database_name": "string",
            "collection_name": "string",
            "github": {
                "git_ref_identifier": {
                    "$oneOf": {
                        "branch": "string",
                        "sha": "string"
                    }
                }
            }
        },
        "created_at": "string"
    }
    ```

- `400 Bad Request` If the request parameters are missing required values or a provided value was malformed.
- `401 Unauthorized` If the `x-chroma-token` header is not present or invalid.
- `403 Forbidden` If the user does not have permission to access this invocation.
- `404 Not Found` If the requested invocation does not exist.
- `500 Internal Server Error` If an unknown error occurs while retrieving the invocation.

### List Invocations

List invocations with optional filtering.

**Method**

    GET

**Endpoint**

    https://sync.trychroma.com/api/v1/invocations

**Required Headers**
- `x-chroma-token` must carry the caller’s Chroma Cloud API key.

**Query Parameters**
- `source_id` allows callers to optionally filter invocations by their source.
- `status` allows callers to optionally filter invocations by their status. If provided, must be one of `[pending, processing, complete, failed, cancelled]`.
- `source_type` allows callers to optionally filter invocations by their source’s type. If provided, must be one of `[github]`.
- `github.app_id` allows callers to optionally filter invocations on sources of the GitHub repository source type by their source’s app ID. If provided, `source_type` must be `github`.
- `github.repository` allows callers to optionally filter invocations on sources of the GitHub repository source type by their source’s repository. If provided, `source_type` must be `github`.
- `limit` specifies the maximum number of results to return. Defaults to 100.
- `offset` specifies the number of results to skip before starting to assemble the list of returned invocations when sorting by the invocations’ `created_at` timestamps. Defaults to 0.
- `order_by` indicates whether to perform sorting by the invocations’ `created_at` timestamps in ascending or descending order. If provided, must be one of `[ASC, DESC]`. Defaults to `DESC`.

**Responses**
- `200 OK` If the request does not encounter any errors while listing invocations.

    ```json
    [
        {
            "invocation_id": "string",
            "status": {
                "$oneOf": {
                    "pending",
                    "processing",
                    "complete": {
                        "finished_at": "string"
                    },
                    "failed": {
                        "error": "string"
                    },
                    "cancelled"
                }
            },
            "source_id": "string",
            "metadata": {
                "database_name": "string",
                "collection_name": "string",
                "github": {
                    "git_ref_identifier": {
                        "$oneOf": {
                            "branch": "string",
                            "sha": "string"
                        }
                    }
                }
            },
            "created_at": "string"
        }
        ...
    ]
    ```

- `400 Bad Request` If the request parameters are missing required values or a provided value was malformed.
- `401 Unauthorized` If the `x-chroma-token` header is not present or invalid.
- `403 Forbidden` If the user does not have permission to list invocations.
- `500 Internal Server Error` If an unknown error occurs while retrieving invocations.

### Cancel Pending Invocation

Cancel an invocation that is in the `pending` state. Invocations not in this state cannot be cancelled.

**Method**

    PUT

**Endpoint**

    https://sync.trychroma.com/api/v1/invocations/{invocation_id}

**Required Headers**
- `x-chroma-token` must carry the caller’s Chroma Cloud API key.

**Request Parameters**
- `invocation_id` is the ID of the invocation to cancel.

**Responses**
- `202 Accepted` If the invocation is cancelled.
- `400 Bad Request` If the request parameters are missing required values or a provided value was malformed.
- `401 Unauthorized` If the `x-chroma-token` header is not present or invalid.
- `403 Forbidden` If the user does not have permission to cancel this invocation.
- `404 Not Found` If the provided invocation does not exist.
- `412 Precondition Not Met` If the invocation is not in the `pending` state.
- `500 Internal Server Error` If an unknown error occurs while cancelling the invocation.

# Walkthrough
