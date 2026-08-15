# type: ignore
import os
import shutil
import tempfile

import pytest

import chromadb
from chromadb.config import Settings


@pytest.fixture
def persist_dir():
    return tempfile.mkdtemp()


@pytest.fixture
def local_persist_api(persist_dir):
    client = chromadb.Client(
        Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            allow_reset=True,
            is_persistent=True,
            persist_directory=persist_dir,
        ),
    )
    yield client
    client.clear_system_cache()
    if os.path.exists(persist_dir):
        shutil.rmtree(persist_dir, ignore_errors=True)


@pytest.fixture
def local_persist_api_cache_bust(persist_dir):
    client = chromadb.Client(
        Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            allow_reset=True,
            is_persistent=True,
            persist_directory=persist_dir,
        ),
    )
    yield client
    client.clear_system_cache()
    if os.path.exists(persist_dir):
        shutil.rmtree(persist_dir, ignore_errors=True)
