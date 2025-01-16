import os
import pytest
from unittest.mock import patch, MagicMock

import chromadb
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.config import System, Settings


@pytest.mark.parametrize("migrations_hash_algorithm", [None, "md5", "sha256"])
@patch("chromadb.api.fastapi.FastAPI")
@patch.dict(os.environ, {}, clear=True)
def test_settings_valid_hash_algorithm(
    api_mock: MagicMock, migrations_hash_algorithm: str
) -> None:
    """
    Ensure that when no hash algorithm or a valid one is provided, the client is set up
    with that value
    """
    if migrations_hash_algorithm:
        settings = chromadb.config.Settings(
            chroma_api_impl="chromadb.api.fastapi.FastAPI",
            is_persistent=True,
            persist_directory="./foo",
            migrations_hash_algorithm=migrations_hash_algorithm,
        )
    else:
        settings = chromadb.config.Settings(
            chroma_api_impl="chromadb.api.fastapi.FastAPI",
            is_persistent=True,
            persist_directory="./foo",
        )

    client = chromadb.Client(settings)

    # Check that the mock was called
    assert api_mock.called

    # Retrieve the arguments with which the mock was called
    # `call_args` returns a tuple, where the first element is a tuple of positional arguments
    # and the second element is a dictionary of keyword arguments. We assume here that
    # the settings object is passed as a positional argument.
    args, kwargs = api_mock.call_args
    passed_settings = args[0] if args else None

    # Check if the default hash algorith was set
    expected_migrations_hash_algorithm = migrations_hash_algorithm or "md5"
    assert passed_settings
    assert (
        getattr(passed_settings.settings, "migrations_hash_algorithm", None)
        == expected_migrations_hash_algorithm
    )
    client.clear_system_cache()


@patch("chromadb.api.fastapi.FastAPI")
@patch.dict(os.environ, {}, clear=True)
def test_settings_invalid_hash_algorithm(mock: MagicMock) -> None:
    """
    Ensure that providing an invalid hash results in a raised exception and the client
    is not called
    """
    with pytest.raises(Exception):
        settings = chromadb.config.Settings(
            chroma_api_impl="chromadb.api.fastapi.FastAPI",
            migrations_hash_algorithm="invalid_hash_alg",
            persist_directory="./foo",
        )

        chromadb.Client(settings)

    assert not mock.called


@pytest.mark.parametrize("migrations_hash_algorithm", ["md5", "sha256"])
@patch("chromadb.db.migrations.verify_migration_sequence")
@patch("chromadb.db.migrations.hashlib")
@patch.dict(os.environ, {}, clear=True)
def test_hashlib_alg(
    hashlib_mock: MagicMock,
    verify_migration_sequence_mock: MagicMock,
    migrations_hash_algorithm: str,
) -> None:
    """
    Test that only the appropriate hashlib functions are called
    """
    db = SqliteDB(
        System(
            Settings(
                migrations="apply",
                allow_reset=True,
                migrations_hash_algorithm=migrations_hash_algorithm,
            )
        )
    )

    # replace the real migration application call with a mock we can check
    db.apply_migration = MagicMock()  # type: ignore [method-assign]

    # we don't want `verify_migration_sequence` to actually run since a) we're not testing that functionality and
    # b) db may be cached between tests, and we're changing the algorithm, so it may fail.
    # Instead, return a fake unapplied migration (expect `apply_migration` to be called after)
    verify_migration_sequence_mock.return_value = ["unapplied_migration"]

    db.start()

    assert db.apply_migration.called

    # Check if the default hash algorith was set
    expected_migrations_hash_algorithm = migrations_hash_algorithm or "md5"

    # check that the right algorithm was used
    if expected_migrations_hash_algorithm == "md5":
        assert hashlib_mock.md5.called
        assert not hashlib_mock.sha256.called
    elif expected_migrations_hash_algorithm == "sha256":
        assert not hashlib_mock.md5.called
        assert hashlib_mock.sha256.called
    else:
        # we only support the algorithms above
        assert False
