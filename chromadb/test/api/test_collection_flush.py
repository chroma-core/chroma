import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Sequence, cast
from uuid import UUID, uuid4

import chromadb
import pytest
from chromadb.api.client import Client
from chromadb.api.models.Collection import Collection
from chromadb.ingest.impl.utils import create_topic_name
from chromadb.types import Collection as CollectionModel


class FlushClient:
    def __init__(self) -> None:
        self.flushed_collection_ids: list[UUID] = []

    def _flush(self, collection_id: UUID) -> None:
        self.flushed_collection_ids.append(collection_id)


def test_collection_flush_routes_to_internal_api() -> None:
    collection_id = uuid4()
    client = FlushClient()
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=CollectionModel(
            id=collection_id,
            name="flush-test",
            configuration_json={},
            serialized_schema=None,
            metadata=None,
            dimension=None,
            tenant="default_tenant",
            database="default_database",
        ),
        embedding_function=None,
    )

    collection.flush()

    assert client.flushed_collection_ids == [collection_id]


def _persistent_flush_state(
    persist_directory: str, collection_id: UUID
) -> tuple[int, list[int]]:
    with sqlite3.connect(Path(persist_directory) / "chroma.sqlite3") as conn:
        vector_segment = conn.execute(
            "SELECT id FROM segments WHERE collection = ? AND scope = 'VECTOR'",
            (str(collection_id),),
        ).fetchone()
        assert vector_segment is not None

        checkpoint_row = conn.execute(
            "SELECT seq_id FROM max_seq_id WHERE segment_id = ?",
            (vector_segment[0],),
        ).fetchone()
        checkpoint = int(checkpoint_row[0]) if checkpoint_row is not None else 0

        topic = create_topic_name("default", "default", collection_id)
        wal_offsets = [
            int(row[0])
            for row in conn.execute(
                "SELECT seq_id FROM embeddings_queue WHERE topic = ? ORDER BY seq_id",
                (topic,),
            ).fetchall()
        ]

    return checkpoint, wal_offsets


def test_collection_flush_persists_and_purges_below_threshold() -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")

    ids = ["id-1", "id-2", "id-3"]
    embeddings: list[Sequence[float]] = [
        [1.0, 0.0],
        [0.0, 1.0],
        [1.0, 1.0],
    ]

    with tempfile.TemporaryDirectory() as persist_directory:
        client = cast(Client, chromadb.PersistentClient(path=persist_directory))
        try:
            collection = client.create_collection(
                "flush-test",
                metadata={"hnsw:batch_size": 3, "hnsw:sync_threshold": 100},
            )
            collection.upsert(ids=ids, embeddings=embeddings)

            checkpoint_before, wal_before = _persistent_flush_state(
                persist_directory, collection.id
            )
            assert checkpoint_before == 0
            assert len(wal_before) == len(ids)
            assert all(offset > checkpoint_before for offset in wal_before)

            collection.flush()

            checkpoint_after, wal_after = _persistent_flush_state(
                persist_directory, collection.id
            )
            assert checkpoint_after == max(wal_before)
            # SQLite retains the checkpoint record so future sequence IDs are not
            # reused, but no records remain beyond the materialized checkpoint.
            assert wal_after == [checkpoint_after]

            # With no pending WAL records, a second flush is an idempotent no-op.
            collection.flush()
            assert _persistent_flush_state(persist_directory, collection.id) == (
                checkpoint_after,
                wal_after,
            )
        finally:
            client.close()
            client.clear_system_cache()

        reopened_client = cast(
            Client, chromadb.PersistentClient(path=persist_directory)
        )
        try:
            reopened = reopened_client.get_collection("flush-test")
            result = reopened.query(
                query_embeddings=[embeddings[0]], n_results=len(ids)
            )
            assert set(result["ids"][0]) == set(ids)
        finally:
            reopened_client.close()
            reopened_client.clear_system_cache()
