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
- `network_info` - Shows network related information. Possible values `True` or `False` (default `False`)
- `env_vars` - Shows environment variables. Possible values `True` or `False` (default `False`). WARNING: Use with
  caution as this may expose sensitive information.
- `collections_info` - Shows information about the collections (names, ids and document counts). Possible values `True`
  or `False` (default `False`)

By default, the endpoint will display the following information (example below):

- `chroma_version` - The version of Chroma
- `chroma_settings` - The settings used to start Chroma
- `datetime` - The current date and time
- `persist_directory` - The directory where Chroma is storing its data

For security reasons the endpoint will be disabled by default. It can be enabled by setting
the `CHROMA_SERVER_ENV_ENDPOINT_ENABLED=1` on the server.

We also suggest the introduction of two cli commands:

- `chroma env` that will print the system information to the console.
- `chroma rstat` that will continuously print CPU and memory usage statistics to the console.

### Example Usage

```bash
chroma env --remote http://localhost:8000 --path ./chroma --collections-info

===================================== Remote system info =====================================
{
    'chroma_version': '0.4.10',
    'chroma_settings': {
        'environment': '',
        'chroma_db_impl': None,
        'chroma_api_impl': 'chromadb.api.segment.SegmentAPI',
        'chroma_telemetry_impl': 'chromadb.telemetry.posthog.Posthog',
        'chroma_sysdb_impl': 'chromadb.db.impl.sqlite.SqliteDB',
        'chroma_producer_impl': 'chromadb.db.impl.sqlite.SqliteDB',
        'chroma_consumer_impl': 'chromadb.db.impl.sqlite.SqliteDB',
        'chroma_segment_manager_impl': 'chromadb.segment.impl.manager.local.LocalSegmentManager',
        'tenant_id': 'default',
        'topic_namespace': 'default',
        'is_persistent': True,
        'persist_directory': '/chroma/chroma',
        'chroma_server_host': None,
        'chroma_server_headers': None,
        'chroma_server_http_port': None,
        'chroma_server_ssl_enabled': False,
        'chroma_server_api_default_path': '/api/v1',
        'chroma_server_grpc_port': None,
        'chroma_server_cors_allow_origins': [],
        'chroma_server_auth_provider': '',
        'chroma_server_auth_configuration_provider': None,
        'chroma_server_auth_configuration_file': None,
        'chroma_server_auth_credentials_provider': '*****',
        'chroma_server_auth_credentials_file': '*****',
        'chroma_server_auth_credentials': '*****',
        'chroma_client_auth_provider': None,
        'chroma_server_auth_ignore_paths': {'/api/v1': ['GET'], '/api/v1/heartbeat': ['GET'], '/api/v1/version': ['GET']},
        'chroma_client_auth_credentials_provider': '*****',
        'chroma_client_auth_protocol_adapter': 'chromadb.auth.providers.RequestsClientAuthProtocolAdapter',
        'chroma_client_auth_credentials_file': '*****',
        'chroma_client_auth_credentials': '*****',
        'chroma_client_auth_token_transport_header': None,
        'chroma_server_auth_token_transport_header': None,
        'chroma_server_env_endpoint_enabled': True,
        'anonymized_telemetry': True,
        'allow_reset': False,
        'migrations': 'apply'
    },
    'datetime': '2023-09-16T10:42:26.743341',
    'persist_directory': '/chroma/chroma',
    'python_version': '3.10.13',
    'os': 'Linux',
    'os_version': '5.15.49-linuxkit-pr',
    'os_release': 'Debian GNU/Linux 12 (bookworm)',
    'memory_info': {'free_memory': 10553921536, 'total_memory': 12544233472, 'process_memory': {'rss': 77250560, 'vms': 491360256}},
    'cpu_info': {'architecture': 'aarch64', 'number_of_cpus': 6, 'cpu_usage': 16.1},
    'collections_info': [{'name': 'BO000', 'id': '60665947-95dd-4acd-8f1f-ad2f6f2a9f55', 'count': 0, 'metadata': None}]
}
===================================== Local system info =====================================
Local persistent client with path: ./chroma
{
    'chroma_version': '0.4.10',
    'chroma_settings': {
        'environment': '',
        'chroma_db_impl': None,
        'chroma_api_impl': 'chromadb.api.segment.SegmentAPI',
        'chroma_telemetry_impl': 'chromadb.telemetry.posthog.Posthog',
        'chroma_sysdb_impl': 'chromadb.db.impl.sqlite.SqliteDB',
        'chroma_producer_impl': 'chromadb.db.impl.sqlite.SqliteDB',
        'chroma_consumer_impl': 'chromadb.db.impl.sqlite.SqliteDB',
        'chroma_segment_manager_impl': 'chromadb.segment.impl.manager.local.LocalSegmentManager',
        'tenant_id': 'default',
        'topic_namespace': 'default',
        'is_persistent': True,
        'persist_directory': './chroma',
        'chroma_server_host': None,
        'chroma_server_headers': None,
        'chroma_server_http_port': None,
        'chroma_server_ssl_enabled': False,
        'chroma_server_api_default_path': '/api/v1',
        'chroma_server_grpc_port': None,
        'chroma_server_cors_allow_origins': [],
        'chroma_server_auth_provider': None,
        'chroma_server_auth_configuration_provider': None,
        'chroma_server_auth_configuration_file': None,
        'chroma_server_auth_credentials_provider': '*****',
        'chroma_server_auth_credentials_file': '*****',
        'chroma_server_auth_credentials': '*****',
        'chroma_client_auth_provider': None,
        'chroma_server_auth_ignore_paths': {'/api/v1': ['GET'], '/api/v1/heartbeat': ['GET'], '/api/v1/version': ['GET']},
        'chroma_client_auth_credentials_provider': '*****',
        'chroma_client_auth_protocol_adapter': 'chromadb.auth.providers.RequestsClientAuthProtocolAdapter',
        'chroma_client_auth_credentials_file': '*****',
        'chroma_client_auth_credentials': '*****',
        'chroma_client_auth_token_transport_header': None,
        'chroma_server_auth_token_transport_header': None,
        'chroma_server_env_endpoint_enabled': False,
        'anonymized_telemetry': True,
        'allow_reset': False,
        'migrations': 'apply'
    },
    'datetime': '2023-09-16T13:42:27.852961',
    'persist_directory': './chroma',
    'python_version': '3.10.10',
    'os': 'Darwin',
    'os_version': '22.6.0',
    'os_release': 'ProductName: macOS ProductVersion: 13.5 BuildVersion: 22G74',
    'memory_info': {'free_memory': 5898862592, 'total_memory': 34359738368, 'process_memory': {'rss': 73695232, 'vms': 418586836992}},
    'cpu_info': {'architecture': 'arm64', 'number_of_cpus': 12, 'cpu_usage': 48.9},
    'collections_info': [{'name': 'BO000', 'id': UUID('60665947-95dd-4acd-8f1f-ad2f6f2a9f55'), 'count': 0, 'metadata': None}]
}
```

Using with cURL:

```bash
curl http://localhost:8000/api/v1/env?python_version=True&os_info=True&memory_info=True&cpu_info=True&disk_info=True&network_info=True&env_vars=True&collections_info=True | jq
{
  "chroma_version": "0.4.10",
  "chroma_settings": {
    "environment": "",
    "chroma_db_impl": null,
    "chroma_api_impl": "chromadb.api.segment.SegmentAPI",
    "chroma_telemetry_impl": "chromadb.telemetry.posthog.Posthog",
    "chroma_sysdb_impl": "chromadb.db.impl.sqlite.SqliteDB",
    "chroma_producer_impl": "chromadb.db.impl.sqlite.SqliteDB",
    "chroma_consumer_impl": "chromadb.db.impl.sqlite.SqliteDB",
    "chroma_segment_manager_impl": "chromadb.segment.impl.manager.local.LocalSegmentManager",
    "tenant_id": "default",
    "topic_namespace": "default",
    "is_persistent": true,
    "persist_directory": "/chroma/chroma",
    "chroma_server_host": null,
    "chroma_server_headers": null,
    "chroma_server_http_port": null,
    "chroma_server_ssl_enabled": false,
    "chroma_server_api_default_path": "/api/v1",
    "chroma_server_grpc_port": null,
    "chroma_server_cors_allow_origins": [],
    "chroma_server_auth_provider": "",
    "chroma_server_auth_configuration_provider": null,
    "chroma_server_auth_configuration_file": null,
    "chroma_server_auth_credentials_provider": "*****",
    "chroma_server_auth_credentials_file": "*****",
    "chroma_server_auth_credentials": "*****",
    "chroma_client_auth_provider": null,
    "chroma_server_auth_ignore_paths": {
      "/api/v1": [
        "GET"
      ],
      "/api/v1/heartbeat": [
        "GET"
      ],
      "/api/v1/version": [
        "GET"
      ]
    },
    "chroma_client_auth_credentials_provider": "*****",
    "chroma_client_auth_protocol_adapter": "chromadb.auth.providers.RequestsClientAuthProtocolAdapter",
    "chroma_client_auth_credentials_file": "*****",
    "chroma_client_auth_credentials": "*****",
    "chroma_client_auth_token_transport_header": null,
    "chroma_server_auth_token_transport_header": null,
    "chroma_server_env_endpoint_enabled": true,
    "anonymized_telemetry": true,
    "allow_reset": false,
    "migrations": "apply"
  },
  "datetime": "2023-09-16T10:58:32.271390",
  "persist_directory": "/chroma/chroma",
  "python_version": "3.10.13",
  "os": "Linux",
  "os_version": "5.15.49-linuxkit-pr",
  "os_release": "Debian GNU/Linux 12 (bookworm)",
  "memory_info": {
    "free_memory": 10681249792,
    "total_memory": 12544233472,
    "process_memory": {
      "rss": 77561856,
      "vms": 491118592
    }
  },
  "cpu_info": {
    "architecture": "aarch64",
    "number_of_cpus": 6,
    "cpu_usage": 0.5
  },
  "disk_info": {
    "total_space": 62671097856,
    "used_space": 54497701888,
    "free_space": 4956680192
  },
  "network_info": {
    "lo": [
      "127.0.0.1"
    ],
    "eth0": [
      "192.168.112.2"
    ],
    "tunl0": [],
    "ip6tnl0": []
  },
  "env_vars": {
    "PATH": "/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "HOSTNAME": "b9a9cb586456",
    "CHROMA_SERVER_AUTH_CREDENTIALS": "*****",
    "CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER": "*****",
    "PERSIST_DIRECTORY": "/chroma/chroma",
    "chroma_server_env_endpoint_enabled": "1",
    "IS_PERSISTENT": "TRUE",
    "CHROMA_SERVER_AUTH_PROVIDER": "*****",
    "CHROMA_SERVER_AUTH_CREDENTIALS_FILE": "*****",
    "LANG": "C.UTF-8",
    "GPG_KEY": "*****",
    "PYTHON_VERSION": "3.10.13",
    "PYTHON_PIP_VERSION": "23.0.1",
    "PYTHON_SETUPTOOLS_VERSION": "65.5.1",
    "PYTHON_GET_PIP_URL": "https://github.com/pypa/get-pip/raw/9af82b715db434abb94a0a6f3569f43e72157346/public/get-pip.py",
    "PYTHON_GET_PIP_SHA256": "45a2bb8bf2bb5eff16fdd00faef6f29731831c7c59bd9fc2bf1f3bed511ff1fe",
    "HOME": "/root"
  },
  "collections_info": [
    {
      "name": "BO000",
      "id": "60665947-95dd-4acd-8f1f-ad2f6f2a9f55",
      "count": 0,
      "metadata": null
    }
  ]
}
```

```bash
chroma rstat --remote http://localhost:8000 --interval 5
0.3 %   77.79 MB
0.3 %   77.79 MB
0.2 %   77.79 MB
0.3 %   77.79 MB
^C
Aborted.
```

> Note: To get friendlier version of the

## Proposed Changes

The proposed changes are mentioned in the public interfaces.

### Future Work

- Prevent the endpoint from being enabled in production environments or if auth is not enabled.
- Sanitation of outputs to avoid disclosing sensitive information.
- Security testing

## Compatibility, Deprecation, and Migration Plan

This change is backward compatible.

## Test Plan

We plan to modify unit tests to accommodate the change and use system tests to verify
this API change is backward compatible.

## Rejected Alternatives

TBD
