from uuid import uuid4

from chromadb.config import Settings, System
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.segment.impl.metadata.sqlite import SqliteMetadataSegment


def test_large_nul_run_does_not_corrupt_fts5_index() -> None:
    """Regression test for gh#7388.

    A large run of embedded NUL bytes in ``chroma:document`` corrupts the FTS5
    inverted index for the entire collection.  The fix sanitizes the document
    *before* it reaches the FTS table, replacing NUL bytes with spaces so that
    tokenization is preserved.

    The reporter in #7388 observed that a single NUL byte does **not** trigger
    the corruption — only a run of ~10 k bytes does — so this test reproduces the
    original payload rather than a single ``\\x00``.
    """
    system = System(
        Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            is_persistent=False,
            allow_reset=True,
        )
    )
    system.start()

    try:
        db = system.instance(SqliteDB)
        segment_id = uuid4()
        metadata_segment = SqliteMetadataSegment(
            system,
            {
                "id": segment_id,
                "type": "urn:chroma:segment/metadata/sqlite",
                "scope": "METADATA",
                "topic": "test",
                "collection": uuid4(),
            },
        )

        # Reproduce the payload from #7388: a ~10 k NUL run between two tokens.
        document = "before-nuls " + "\x00" * 10291 + " after-nuls"

        with db.tx() as cur:
            cur.execute(
                "INSERT INTO embeddings (segment_id, embedding_id, seq_id) "
                "VALUES (?, ?, ?)",
                (str(segment_id), "id", 1),
            )
            metadata_segment._insert_metadata(cur, 1, {"chroma:document": document})

        with db.tx() as cur:
            # The FTS5 index must remain intact (the core of #7388).
            integrity = cur.execute("PRAGMA quick_check").fetchone()
            assert integrity[0] == "ok"

            # The document must be present in the FTS table without NUL bytes.
            row = cur.execute(
                "SELECT string_value FROM embedding_fulltext_search "
                "WHERE rowid = ?",
                (1,),
            ).fetchone()
            assert row is not None
            assert "\x00" not in row[0]
    finally:
        system.stop()


def test_nul_replaced_with_space_preserves_tokenization() -> None:
    """Pin the decision to replace NUL with a space, not the empty string.

    gh#7388's large-NUL payload has spaces on both sides of the NUL run, so it
    cannot tell a space replacement from an empty-string replacement apart. A
    single ``\\x00`` between two tokens can: replacing with a space keeps
    ``prefix`` and ``suffix`` independently searchable, whereas the empty string
    would collapse them into ``prefixsuffix`` (matching neither half).
    """
    system = System(
        Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            is_persistent=False,
            allow_reset=True,
        )
    )
    system.start()

    try:
        db = system.instance(SqliteDB)
        segment_id = uuid4()
        metadata_segment = SqliteMetadataSegment(
            system,
            {
                "id": segment_id,
                "type": "urn:chroma:segment/metadata/sqlite",
                "scope": "METADATA",
                "topic": "test",
                "collection": uuid4(),
            },
        )

        with db.tx() as cur:
            cur.execute(
                "INSERT INTO embeddings (segment_id, embedding_id, seq_id) "
                "VALUES (?, ?, ?)",
                (str(segment_id), "id", 1),
            )
            metadata_segment._insert_metadata(
                cur, 1, {"chroma:document": "prefix\x00suffix"}
            )

        with db.tx() as cur:
            # Both halves must remain independently searchable after the NUL is
            # replaced with a space ("prefix suffix" still matches "prefix").
            hits = cur.execute(
                "SELECT count(*) FROM embedding_fulltext_search "
                "WHERE embedding_fulltext_search MATCH ?",
                ('"prefix"',),
            ).fetchone()[0]
            assert hits == 1
    finally:
        system.stop()

