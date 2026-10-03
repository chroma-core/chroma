from uuid import uuid4

import pytest

from chromadb.api.collection_configuration import CreateCollectionConfiguration
from chromadb.config import System
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.errors import NotFoundError, UniqueConstraintError


def test_create_database_with_missing_tenant_raises_not_found(
    sqlite: System,
) -> None:
    sysdb = sqlite.instance(SqliteDB)
    with pytest.raises(NotFoundError) as exc_info:
        sysdb.create_database(id=uuid4(), name="mydb", tenant="ghost_tenant")
    assert "Tenant ghost_tenant not found" in str(exc_info.value)


def test_create_database_with_existing_tenant_still_reports_duplicates(
    sqlite: System,
) -> None:
    sysdb = sqlite.instance(SqliteDB)
    sysdb.create_tenant("tenant_a")
    sysdb.create_database(id=uuid4(), name="dup", tenant="tenant_a")
    with pytest.raises(UniqueConstraintError) as exc_info:
        sysdb.create_database(id=uuid4(), name="dup", tenant="tenant_a")
    assert "already exists" in str(exc_info.value)


def test_create_collection_with_missing_database_raises_not_found(
    sqlite: System,
) -> None:
    sysdb = sqlite.instance(SqliteDB)
    sysdb.create_tenant("tenant_b")
    with pytest.raises(NotFoundError) as exc_info:
        sysdb.create_collection(
            id=uuid4(),
            name="mycol",
            schema=None,
            configuration=CreateCollectionConfiguration(),
            segments=[],
            tenant="tenant_b",
            database="ghost_db",
        )
    assert "Database ghost_db not found for tenant tenant_b" in str(exc_info.value)
