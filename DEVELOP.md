# Development Instructions

This project uses the testing, build and release standards specified
by the PyPA organization and documented at
https://packaging.python.org.

## Setup

Because of the dependencies it relies on (like `pytorch`), this project does not support Python version >3.10.0.

Set up a virtual environment and install the project's requirements
and dev requirements:

```
python3 -m venv venv      # Only need to do this once
source venv/bin/activate  # Do this each time you use a new shell for the project
pip install -r requirements.txt
pip install -r requirements_dev.txt
pre-commit install # install the precommit hooks
```

You can also install `chromadb` the `pypi` package locally and in editable mode with `pip install -e .`.

## Running Chroma

Chroma can be run via 3 modes:
1. Standalone and in-memory:
```python
import chromadb
api = chromadb.Client()
print(api.heartbeat())
```

2. Standalone and in-memory with persistence:

This by default saves your db and your indexes to a `.chroma` directory and can also load from them.
```python
import chromadb
from chromadb.config import Settings
api = chromadb.Client(Settings(chroma_db_impl="duckdb+parquet",
                      persist_directory="/path/to/persist/directory"))
print(api.heartbeat())
```


3. With a persistent backend and a small frontend client

Run `docker-compose up -d --build`
```python
import chromadb
from chromadb.config import Settings
api = chromadb.Client(Settings(chroma_api_impl="rest",
                              chroma_server_host="localhost",
                              chroma_server_http_port="8000") )

print(api.heartbeat())
```

## Testing

Unit tests are in the `/chromadb/test` directory.

To run unit tests using your current environment, run `pytest`.

## Manual Build

To manually build a distribution, run `python -m build`.

The project's source and wheel distributions will be placed in the `dist` directory.

## Manual Release

Not yet implemented.

## Versioning

This project uses PyPA's `setuptools_scm` module to determine the
version number for build artifacts, meaning the version number is
derived from Git rather than hardcoded in the repository. For full
details, see the
[documentation for setuptools_scm](https://github.com/pypa/setuptools_scm/).

In brief, version numbers are generated as follows:

- If the current git head is tagged, the version number is exactly the
  tag (e.g, `0.0.1`).
- If the the current git head is a clean checkout, but is not tagged,
  the version number is a patch version increment of the most recent
  tag, plus `devN` where N is the number of commits since the most
  recent tag. For example, if there have been 5 commits since the
  `0.0.1` tag, the generated version will be `0.0.2-dev5`.
- If the current head is not a clean checkout, a `+dirty` local
  version will be appended to the version number. For example,
  `0.0.2-dev5+dirty`.

At any point, you can manually run `python -m setuptools_scm` to see
what version would be assigned given your current state.

## Continuous Integration

This project uses Github Actions to run unit tests automatically upon
every commit to the main branch. See the documentation for Github
Actions and the flow definitions in `.github/workflows` for details.

## Continuous Delivery

Not yet implemented.
