import os
import pytest
from unittest.mock import patch, Mock

import chromadb


@pytest.mark.parametrize("migrations_hash_algorithm", [None, "md5", "sha256"])
@patch("chromadb.api.fastapi.FastAPI", autospec=True)
@patch.dict(os.environ, {}, clear=True)
def test_settings_valid_hash_algorithm(
    mock: Mock, migrations_hash_algorithm: str
) -> None:
    """
    Ensure that when no hash algorithm or a valid one is provided, the client is set up
    with that value
    """
    if migrations_hash_algorithm:
        settings = chromadb.config.Settings(
            chroma_api_impl="chromadb.api.fastapi.FastAPI",
            migrations_hash_algorithm=migrations_hash_algorithm,
        )
    else:
        settings = chromadb.config.Settings(
            chroma_api_impl="chromadb.api.fastapi.FastAPI",
        )

    client = chromadb.Client(settings)

    # Check that the mock was called
    assert mock.called

    # Retrieve the arguments with which the mock was called
    # `call_args` returns a tuple, where the first element is a tuple of positional arguments
    # and the second element is a dictionary of keyword arguments. We assume here that
    # the settings object is passed as a positional argument.
    args, kwargs = mock.call_args
    passed_settings = args[0] if args else None

    # Check if the default hash algorith was set
    expected_migrations_hash_algorithm = migrations_hash_algorithm or "md5"
    assert passed_settings
    assert (
        getattr(passed_settings.settings, "migrations_hash_algorithm", None)
        == expected_migrations_hash_algorithm
    )

    client.clear_system_cache()


@patch("chromadb.api.fastapi.FastAPI", autospec=True)
@patch.dict(os.environ, {}, clear=True)
def test_settings_invalid_hash_algorithm(mock: Mock) -> None:
    """
    Ensure that providing an invalid hash results in a raised exception and the client
    is not called
    """
    with pytest.raises(Exception):
        settings = chromadb.config.Settings(
            chroma_api_impl="chromadb.api.fastapi.FastAPI",
            migrations_hash_algorithm="invalid_hash_alg",
        )

        chromadb.Client(settings)

    assert not mock.called
