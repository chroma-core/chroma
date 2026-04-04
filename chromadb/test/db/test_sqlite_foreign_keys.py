import tempfile
from unittest.mock import patch

from chromadb.config import Settings, System
from chromadb.db.impl.sqlite import SqliteDB


def _make_system(persist_directory: str) -> System:
    settings = Settings(
        chroma_api_impl="chromadb.api.segment.SegmentAPI",
        chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
        persist_directory=persist_directory,
        is_persistent=True,
        allow_reset=True,
        anonymized_telemetry=False,
    )
    system = System(settings)
    return system


def test_foreign_keys_enabled_outside_transaction() -> None:
    """Verify PRAGMA foreign_keys = ON is effective (set outside transaction)."""
    save_path = tempfile.TemporaryDirectory()
    system = _make_system(save_path.name)
    system.start()
    try:
        db = system.instance(SqliteDB)
        conn = db._conn_pool.connect()
        result = conn.execute("PRAGMA foreign_keys").fetchone()
        db._conn_pool.return_to_pool(conn)
        assert result[0] == 1, "foreign_keys pragma should be ON"
    finally:
        system.stop()
        save_path.cleanup()


def test_foreign_keys_enabled_in_transaction() -> None:
    """Verify foreign_keys is set before BEGIN in TxWrapper."""
    save_path = tempfile.TemporaryDirectory()
    system = _make_system(save_path.name)
    system.start()
    try:
        db = system.instance(SqliteDB)
        with db.tx() as cur:
            cur.execute("PRAGMA foreign_keys")
            result = cur.fetchone()
            assert result[0] == 1, "foreign_keys should be ON inside transaction"
    finally:
        system.stop()
        save_path.cleanup()


def test_foreign_key_check_no_violations() -> None:
    """Verify _check_foreign_key_integrity runs without error on clean DB."""
    save_path = tempfile.TemporaryDirectory()
    system = _make_system(save_path.name)
    system.start()
    try:
        db = system.instance(SqliteDB)
        # Should not raise
        db._check_foreign_key_integrity()
    finally:
        system.stop()
        save_path.cleanup()


def test_foreign_key_check_detects_violations() -> None:
    """Verify telemetry is sent when FK violations exist."""
    save_path = tempfile.TemporaryDirectory()
    system = _make_system(save_path.name)
    system.start()
    try:
        db = system.instance(SqliteDB)

        # Create a FK violation by inserting orphaned data with FK enforcement off
        conn = db._conn_pool.connect()
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            # Create test tables with a foreign key relationship
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _fk_test_parent (id INTEGER PRIMARY KEY)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _fk_test_child "
                "(id INTEGER PRIMARY KEY, parent_id INTEGER "
                "REFERENCES _fk_test_parent(id))"
            )
            # Insert orphan row (parent_id=999 doesn't exist in parent table)
            conn.execute(
                "INSERT INTO _fk_test_child (id, parent_id) VALUES (1, 999)"
            )
        finally:
            conn.execute("PRAGMA foreign_keys = ON")
            db._conn_pool.return_to_pool(conn)

        # Now check that _check_foreign_key_integrity detects the violation
        with patch.object(db._product_telemetry_client, "capture") as mock_capture:
            db._check_foreign_key_integrity()
            assert mock_capture.called, (
                "Telemetry capture should be called when FK violations exist"
            )
            event = mock_capture.call_args[0][0]
            assert event.num_violations >= 1
            assert "_fk_test_child" in event.tables_affected

    finally:
        system.stop()
        save_path.cleanup()
