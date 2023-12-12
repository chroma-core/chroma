# CIP-12062023: System Info

## Status

Current Status: `Under Discussion`

## Motivation

Currently, a lot of the support discussions in Discord revolve around gathering information about the user's operating
environment. This information is crucial for debugging and troubleshooting issues. We want to make it easier for users
to provide this information.

## Public Interfaces

This proposal introduces a new `API` endpoint `/api/v1/env` that will return a dictionary with system information based
on flags provided by the user.


For security reasons the endpoint will be protected by auth. It is recommended to always enable auth when running
Chroma in production or otherwise publicly exposed environment.

We also suggest the introduction of two cli commands:

- `chroma env` that will print the system information to the console.

### Example Usage

#### Python Client

```python
import chromadb
import json
client = chromadb.PersistentClient(path="testchroma")
print(json.dumps(client.env(), indent=4))
```

Producing the following output:

```json
{
    "client": {
        "chroma_version": "0.4.18",
        "python_version": "3.11.2",
        "is_persistent": true,
        "api": "chromadb.api.segment.SegmentAPI",
        "datetime": "2023-12-12T14:51:50.634453",
        "os": "Darwin",
        "os_version": "22.6.0",
        "os_release": "ProductName: macOS ProductVersion: 13.6.2 BuildVersion: 22G320",
        "memory_free": 6750699520,
        "memory_total": 34359738368,
        "process_memory_rss": 124862464,
        "process_memory_vms": 420594466816,
        "cpu_architecture": "arm64",
        "cpu_count": 12,
        "cpu_usage": 27.3,
        "persistent_disk_free": 61767798784,
        "persistent_disk_total": 994662584320,
        "persistent_disk_used": 861841027072,
        "mode": "persistent client"
    },
    "server": null
}
```

#### CLI

```bash
chroma env --remote http://localhost:8000
================================== Remote Sever system info ======================================
{
    "chroma_version": "0.4.18",
    "python_version": "3.10.13",
    "is_persistent": true,
    "api": "chromadb.api.segment.SegmentAPI",
    "datetime": "2023-12-12T12:48:03.812470",
    "os": "Linux",
    "os_version": "6.5.11-linuxkit",
    "os_release": "Debian GNU/Linux 12 (bookworm)",
    "cpu_architecture": "aarch64",
    "cpu_count": 12,
    "cpu_usage": 0.4,
    "memory_free": 10335645696,
    "memory_total": 12538855424,
    "process_memory_rss": 129458176,
    "process_memory_vms": 718258176,
    "persistent_disk_total": 62671097856,
    "persistent_disk_used": 33626783744,
    "persistent_disk_free": 25827598336,
    "mode": "server single node server"
}
================================== Local client system info ======================================
{
    "chroma_version": "0.4.18",
    "python_version": "3.11.2",
    "is_persistent": false,
    "api": "chromadb.api.fastapi.FastAPI",
    "datetime": "2023-12-12T14:48:04.827205",
    "os": "Darwin",
    "os_version": "22.6.0",
    "os_release": "ProductName: macOS ProductVersion: 13.6.2 BuildVersion: 22G320",
    "memory_free": 5945114624,
    "memory_total": 34359738368,
    "process_memory_rss": 92225536,
    "process_memory_vms": 420391944192,
    "cpu_architecture": "arm64",
    "cpu_count": 12,
    "cpu_usage": 30.1,
    "persistent_disk_free": null,
    "persistent_disk_total": null,
    "persistent_disk_used": null,
    "mode": "http client"
}
==================================================================================================
```

#### External Tooling (curl, jq)

```bash
curl "http://localhost:8000/api/v1/env?python_version=True&os_info=True&memory_info=True&cpu_info=True&disk_info=True" | jq
{
  "chroma_version": "0.4.18",
  "is_persistent": true,
  "api": "chromadb.api.segment.SegmentAPI",
  "datetime": "2023-12-06T18:01:36.708217",
  "persist_directory": "./chroma",
  "python_version": "3.11.2",
  "os": "Darwin",
  "os_version": "22.6.0",
  "os_release": "ProductName: macOS ProductVersion: 13.6.2 BuildVersion: 22G320",
  "memory_info": {
    "free_memory": 9057304576,
    "total_memory": 34359738368,
    "process_memory": {
      "rss": 95420416,
      "vms": 420263067648
    }
  },
  "cpu_info": {
    "architecture": "arm64",
    "number_of_cpus": 12,
    "cpu_usage": 12.9
  },
  "disk_info": {
    "total_space": 994662584320,
    "used_space": 871460143104,
    "free_space": 107981352960
  }
}
```

## Proposed Changes

The proposed changes are mentioned in the public interfaces.

### Future Work

- Prevent the endpoint from being enabled in production environments or if auth is not enabled.

## Compatibility, Deprecation, and Migration Plan

This change is backward compatible.

## Test Plan

Tests to be added for API and CLI.

## Rejected Alternatives

TBD
