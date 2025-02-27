import json
import multiprocessing
import os
import packaging
import re
import shutil
import subprocess
import sys
import tempfile
import tqdm
import urllib

from chromadb import RustClient
from chromadb.config import Settings
from chromadb.segment.impl.manager.local import LocalSegmentManager
from chromadb.test.property.test_cross_version_persist import api_import_for_version
from chromadb.test.utils.cross_version import install_version, switch_to_version
from packaging import version
from typing import List
from urllib import request

persist_size = 10000
batch_size = 100
collection_name = "rust_py_compat_test"

version_re = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")

def versions() -> List[str]:
    """Returns the pinned minimum version and the latest version of chromadb."""
    url = "https://pypi.org/pypi/chromadb/json"
    data = json.load(request.urlopen(request.Request(url)))
    versions = list(data["releases"].keys())
    # Older versions on pypi contain "devXYZ" suffixes
    versions = [v for v in versions if version_re.match(v) and version.Version(v) >= version.Version("0.5.3")]
    versions.sort(key=version.Version)
    return versions

def persist_with_old_version(ver: str, path: str):
    print(f"Installing ChromaDB {ver}")
    install_version(ver, {})
    old_modules = switch_to_version(ver, ["pydantic", "numpy", "tokenizers"])
    
    print(f"Initializing client {ver}")
    settings = Settings(
        chroma_api_impl="chromadb.api.segment.SegmentAPI",
        chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
        allow_reset=True,
        is_persistent=True,
        persist_directory=path,
    )
    if version.Version(ver) <= version.Version("0.4.14"):
        settings.chroma_telemetry_impl = "chromadb.telemetry.posthog.Posthog"
    system = old_modules.config.System(settings)
    api = system.instance(api_import_for_version(old_modules, ver))
    system.start()
    api.reset()
    if version.Version(ver) >= version.Version("0.5.4"):    
        api = old_modules.api.client.Client.from_system(system)

    print(f"Persisting data with old client to {path}")
    coll = api.create_collection(collection_name)
    for start in tqdm.tqdm(range(0, persist_size // 2, batch_size)):
        id_vals = range(start, start + batch_size)
        documents = [f"DOC-{i}" for i in id_vals]
        embeddings = [[i, i] for i in id_vals]
        ids = [str(i) for i in id_vals]
        metadatas = [{"int": i, "float": i / 2.0, "str": f"<{i}>"} for i in id_vals]
        coll.add(ids=ids, documents=documents, embeddings=embeddings, metadatas=metadatas)
    assert coll.count() == persist_size // 2
    system.instance(LocalSegmentManager).stop()
    for start in tqdm.tqdm(range(persist_size // 2, persist_size, batch_size)):
        id_vals = range(start, start + batch_size)
        documents = [f"DOC-{i}" for i in id_vals]
        embeddings = [[i, i] for i in id_vals]
        ids = [str(i) for i in id_vals]
        metadatas = [{"int": i, "float": i / 2.0, "str": f"<{i}>"} for i in id_vals]
        coll.add(ids=ids, documents=documents, embeddings=embeddings, metadatas=metadatas)

def verify_collection_content(path: str):
    print("Loading collection from rust client")
    client = RustClient(path=path)
    coll = client.get_collection(collection_name)

    print("Verifying collection content")
    assert coll.count() == persist_size
    records = coll.get(include=["documents", "embeddings", "metadatas"])
    assert records["ids"] == [str(i) for i in range(persist_size)]
    assert records["documents"] == [f"DOC-{i}" for i in range(persist_size)]
    assert all(emb[0] == emb[1] == i for i, emb in enumerate(records["embeddings"]))

if __name__ == "__main__":
    for ver in versions():
        path = tempfile.gettempdir() + "/" + collection_name
        ctx = multiprocessing.get_context("spawn")
        proc_handle = ctx.Process(
            target=persist_with_old_version,
            args=(ver, path),
        )
        proc_handle.start()
        proc_handle.join()
        if proc_handle.exitcode == 0:
            verify_collection_content(path)
        shutil.rmtree(path, ignore_errors=True)
