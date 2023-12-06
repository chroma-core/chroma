# CIP-7: System Info

## Status

Current Status: `Under Discussion`

## Motivation

Currently, a lot of the support discussions in Discord revolve around gathering information about the user's operating
environment. This information is crucial for debugging and troubleshooting issues. We want to make it easier for users
to provide this information.

## Public Interfaces

This proposal introduces a new `API` endpoint `/api/v1/env` that will return a dictionary with system information based
on flags provided by the user.

The endpoint will accept the following flags:

- `python_version` - Shows Python related information. Possible values `True` or `False` (default `True`)
- `os_info` - Shows OS related information. Possible values `True` or `False` (default `True`)
- `memory_info` - Shows memory related information. Possible values `True` or `False` (default `True`)
- `cpu_info` - Shows CPU related information. Possible values `True` or `False` (default `True`)
- `disk_info` - Shows disk related information. Possible values `True` or `False` (default `False`)

By default, the endpoint will display the following information (example below):

- `chroma_version` - The version of Chroma
- `chroma_settings` - The settings used to start Chroma
- `datetime` - The current date and time
- `persist_directory` - The directory where Chroma is storing its data

For security reasons the endpoint will be disabled by default. It can be enabled by setting
the `CHROMA_SERVER_ENV_ENDPOINT_ENABLED=1` on the server.

We also suggest the introduction of two cli commands:

- `chroma env info` that will print the system information to the console.
- `chroma env rstat` that will continuously print CPU and memory usage statistics to the console.

### Example Usage

#### Python Client

```python
import chromadb
client = chromadb.PersistentClient(path="testchroma")
print(client.env())
```

Producing the following output:

```python
{
    'client': {
        'chroma_version': '0.4.18',
        'is_persistent': True,
        'api': 'chromadb.api.segment.SegmentAPI',
        'datetime': '2023-12-06T15:56:26.564277',
        'persist_directory': 'testchroma',
        'python_version': '3.11.2',
        'os': 'Darwin',
        'os_version': '22.6.0',
        'os_release': 'ProductName: macOS ProductVersion: 13.6.2 BuildVersion: 22G320',
        'memory_info': {
            'free_memory': 9616818176,
            'total_memory': 34359738368,
            'process_memory': {
                'rss': 121454592,
                'vms': 420668538880
            }
        },
        'cpu_info': {
            'architecture': 'arm64',
            'number_of_cpus': 12
        },
        'disk_info': {
            'total_space': 994662584320,
            'used_space': 870198599680,
            'free_space': 109242904576
        },
        'mode': 'persistent'
    }
}
```

#### CLI

```bash
chroma env info --remote http://localhost:8000
================================== Remote Sever system info ==================================
{
    "chroma_version": "0.4.18",
    "is_persistent": true,
    "api": "chromadb.api.segment.SegmentAPI",
    "datetime": "2023-12-06T18:00:18.340669",
    "persist_directory": "./chroma",
    "python_version": "3.11.2",
    "os": "Darwin",
    "os_version": "22.6.0",
    "os_release": "ProductName: macOS ProductVersion: 13.6.2 BuildVersion: 22G320",
    "memory_info": {
        "free_memory": 9124806656,
        "total_memory": 34359738368,
        "process_memory": {
            "rss": 95485952,
            "vms": 420263067648
        }
    },
    "cpu_info": {
        "architecture": "arm64",
        "number_of_cpus": 12,
        "cpu_usage": 28.0
    },
    "disk_info": {
        "total_space": 994662584320,
        "used_space": 871452631040,
        "free_space": 107988865024
    }
}
================================== End Remote Sever system info ==================================
================================== Local client system info ==================================
{
    "chroma_version": "0.4.18",
    "is_persistent": false,
    "api": "chromadb.api.fastapi.FastAPI",
    "datetime": "2023-12-06T18:00:19.368348",
    "python_version": "3.11.2",
    "os": "Darwin",
    "os_version": "22.6.0",
    "os_release": "ProductName: macOS ProductVersion: 13.6.2 BuildVersion: 22G320",
    "memory_info": {
        "free_memory": 9214787584,
        "total_memory": 34359738368,
        "process_memory": {
            "rss": 90046464,
            "vms": 420389847040
        }
    },
    "cpu_info": {
        "architecture": "arm64",
        "number_of_cpus": 12,
        "cpu_usage": 29.5
    },
    "mode": "http"
}
================================== End local system info ==================================

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

```bash
chroma env rstat --remote http://localhost:8000 --interval 5
0.3 %   77.79 MB
0.3 %   77.79 MB
0.2 %   77.79 MB
0.3 %   77.79 MB
^C
Aborted.
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
