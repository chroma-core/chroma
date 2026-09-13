"""Tests for PersistentClient settings.persist_directory handling.

Regression tests for https://github.com/chroma-core/chroma/issues/7277
"""
import tempfile
import os


def test_persistent_client_respects_settings_persist_directory() -> None:
    """PersistentClient should respect settings.persist_directory when path is None."""
    import chromadb
    from chromadb.config import Settings

    with tempfile.TemporaryDirectory() as tmpdir:
        custom_path = os.path.join(tmpdir, "custom_chroma")
        settings = Settings()
        settings.persist_directory = custom_path

        # Call PersistentClient without path parameter
        client = chromadb.PersistentClient(settings=settings)

        # Verify the custom path was used
        assert settings.persist_directory == custom_path, (
            f"Expected persist_directory={custom_path}, got {settings.persist_directory}"
        )
        client.reset()


def test_persistent_client_path_overrides_settings() -> None:
    """Path parameter should override settings.persist_directory."""
    import chromadb
    from chromadb.config import Settings

    with tempfile.TemporaryDirectory() as tmpdir:
        settings_path = os.path.join(tmpdir, "settings_chroma")
        param_path = os.path.join(tmpdir, "param_chroma")

        settings = Settings()
        settings.persist_directory = settings_path

        # Call PersistentClient with explicit path
        client = chromadb.PersistentClient(path=param_path, settings=settings)

        # Path parameter should win
        assert settings.persist_directory == param_path, (
            f"Expected persist_directory={param_path}, got {settings.persist_directory}"
        )
        client.reset()


def test_persistent_client_defaults_to_chroma() -> None:
    """Without path or settings, PersistentClient defaults to ./chroma."""
    import chromadb

    # This should not raise
    client = chromadb.PersistentClient()
    client.reset()
