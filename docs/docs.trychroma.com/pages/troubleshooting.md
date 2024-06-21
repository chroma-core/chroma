---
title: "🔍 Troubleshooting"
---

This page is a list of common gotchas or issues and how to fix them.

If you don't see your problem listed here, please also search the [Github Issues](https://github.com/chroma-core/chroma/issues).

## Using .get or .query, embeddings say `None`

This is actually not an error. Embeddings are quite large and heavy to send back. Most application don't use the underlying embeddings and so, by default, chroma does not send them back.

To send them back: add `include=["embeddings", "documents", "metadatas", "distances"]` to your query to return all information.

For example:

```python
results = collection.query(
    query_texts="hello",
    n_results=1,
    include=["embeddings", "documents", "metadatas", "distances"],
)
```

{% note type="tip"  %}
We may change `None` to something else to more clearly communicate why they were not returned.
{% /note %}


## Build error when running `pip install chromadb`

If you encounter an error like this during setup

```
Failed to build hnswlib
ERROR: Could not build wheels for hnswlib, which is required to install pyproject.toml-based projects
```

Try these few tips from the [community](https://github.com/chroma-core/chroma/issues/221):

1. If you get the error: `clang: error: the clang compiler does not support '-march=native'`, set this ENV variable, `export HNSWLIB_NO_NATIVE=1`
2. If on Mac, install/update xcode dev tools, `xcode-select --install`
3. If on Windows, try [these steps](https://github.com/chroma-core/chroma/issues/250#issuecomment-1540934224)

## SQLite

Chroma requires SQLite > 3.35, if you encounter issues with having too low of a SQLite version please try the following.

1. Install the latest version of Python 3.10, sometimes lower versions of python are bundled with older versions of SQLite.
2. If you are on a Linux system, you can install pysqlite3-binary, `pip install pysqlite3-binary` and then override the default
   sqlite3 library before running Chroma with the steps [here](https://gist.github.com/defulmere/8b9695e415a44271061cc8e272f3c300).
   Alternatively you can compile SQLite from scratch and replace the library in your python installation with the latest version as documented [here](https://github.com/coleifer/pysqlite3#building-a-statically-linked-library).
3. If you are on Windows, you can manually download the latest version of SQLite from https://www.sqlite.org/download.html and
   replace the DLL in your python installation's DLLs folder with the latest version. You can find your python installation path by running `os.path.dirname(sys.executable)` in python.
4. If you are using a Debian based Docker container, older Debian versions do not have an up to date SQLite, please use `bookworm` or higher.

##  Illegal instruction (core dumped)

If you encounter an error like this during setup and are using Docker - you may have built the library on a machine with a different CPU architecture than the one you are running it on. Try rebuilding the Docker image on the machine you are running it on.
