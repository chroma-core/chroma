from typing import Dict
from uuid import UUID, uuid4

import pytest

from chromadb.api.collection_configuration import CreateCollectionConfiguration
from chromadb.config import System
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.types import Segment, SegmentScope

TENANT = "default_tenant"


def _row_counts(system: System) -> Dict[str, int]:
    db = system.instance(SqliteDB)
    with db.tx() as cur:
        return {
            table: cur.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "collections",
                "segments",
                "segment_metadata",
                "collection_metadata",
            )
        }


def _create_collection(db: SqliteDB, database: str) -> UUID:
    collection_id = uuid4()
    segment_id = uuid4()
    segment = Segment(
        id=segment_id,
        type="test",
        scope=SegmentScope.VECTOR,
        collection=collection_id,
        metadata=None,
        topic=None,
    )
    db.create_collection(
        id=collection_id,
        name=f"coll-{collection_id}",
        schema=None,
        configuration=CreateCollectionConfiguration(),
        segments=[segment],
        metadata={"foo": "bar"},
        dimension=None,
        get_or_create=False,
        tenant=TENANT,
        database=database,
    )
    # Give the segment a metadata row so segment cleanup is observable.
    db.update_segment(
        collection=collection_id, id=segment_id, metadata={"seg_key": "seg_value"}
    )
    return collection_id


def test_delete_collection_removes_dependent_rows(
    python_sqlite_persistent: System,
) -> None:
    system = python_sqlite_persistent
    db = system.instance(SqliteDB)
    collection_id = _create_collection(db, "default_database")

    before = _row_counts(system)
    assert before["collections"] == 1
    assert before["segments"] == 1
    assert before["segment_metadata"] == 1
    assert before["collection_metadata"] == 1

    db.delete_collection(collection_id, tenant=TENANT, database="default_database")

    after = _row_counts(system)
    assert after["collections"] == 0
    assert after["segments"] == 0
    assert after["segment_metadata"] == 0
    assert after["collection_metadata"] == 0


def test_delete_database_removes_dependent_rows(
    python_sqlite_persistent: System,
) -> None:
    system = python_sqlite_persistent
    db = system.instance(SqliteDB)
    db.create_database(uuid4(), "doomed_db", TENANT)
    _create_collection(db, "doomed_db")
    _create_collection(db, "doomed_db")

    before = _row_counts(system)
    assert before["collections"] == 2
    assert before["segments"] == 2
    assert before["segment_metadata"] == 2
    assert before["collection_metadata"] == 2

    db.delete_database("doomed_db", tenant=TENANT)

    after = _row_counts(system)
    assert after["collections"] == 0
    assert after["segments"] == 0
    assert after["segment_metadata"] == 0
    assert after["collection_metadata"] == 0


def test_delete_segment_removes_metadata_rows(
    python_sqlite_persistent: System,
) -> None:
    system = python_sqlite_persistent
    db = system.instance(SqliteDB)
    collection_id = _create_collection(db, "default_database")
    segment_id = db.get_segments(collection=collection_id)[0]["id"]

    db.delete_segment(collection_id, segment_id)

    counts = _row_counts(system)
    assert counts["segments"] == 0
    assert counts["segment_metadata"] == 0


def test_delete_database_missing_database_raises(
    python_sqlite_persistent: System,
) -> None:
    db = python_sqlite_persistent.instance(SqliteDB)
    with pytest.raises(Exception):
        db.delete_database("no_such_db", tenant=TENANT)
