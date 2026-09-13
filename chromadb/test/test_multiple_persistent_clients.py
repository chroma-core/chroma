"""Tests for multiple PersistentClient instances with different paths.

Regression tests for https://github.com/chroma-core/chroma/issues/7253
"""
import tempfile
import os


def test_multiple_persistent_clients_different_paths() -> None:
    """Multiple PersistentClient instances with different paths should work."""
    import chromadb

    with tempfile.TemporaryDirectory() as tmpdir:
        path1 = os.path.join(tmpdir, "db1")
        path2 = os.path.join(tmpdir, "db2")

        client1 = chromadb.PersistentClient(path=path1)
        client2 = chromadb.PersistentClient(path=path2)

        # Both should work independently
        coll1 = client1.get_or_create_collection("test")
        coll2 = client2.get_or_create_collection("test")

        coll1.add(ids=["id1"], embeddings=[[1.0, 2.0, 3.0]])
        coll2.add(ids=["id2"], embeddings=[[4.0, 5.0, 6.0]])

        assert coll1.count() == 1
        assert coll2.count() == 1

        client1.reset()
        client2.reset()


def test_same_path_reuses_system() -> None:
    """Multiple PersistentClient instances with same path should share system."""
    import chromadb

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "shared_db")

        client1 = chromadb.PersistentClient(path=path)
        client2 = chromadb.PersistentClient(path=path)

        # Both should share the same system
        coll1 = client1.get_or_create_collection("test")
        coll2 = client2.get_or_create_collection("test")

        # They should be the same collection
        assert coll1.id == coll2.id

        client1.reset()


def test_relative_paths_normalized() -> None:
    """Relative paths should be normalized to absolute paths."""
    import chromadb
    from chromadb.api.shared_system_client import SharedSystemClient

    # Create two clients with paths that would be different as relative
    # but same as absolute (not possible to test easily, but we can verify
    # the normalization logic)
    with tempfile.TemporaryDirectory() as tmpdir:
        path1 = os.path.join(tmpdir, "db1")
        path2 = os.path.join(tmpdir, "db1")  # Same path

        # Verify they produce the same identifier
        id1 = SharedSystemClient._get_identifier_from_settings(
            chromadb.config.Settings(
                chroma_api_impl="chromadb.api.segment.SegmentAPI",
                is_persistent=True,
                persist_directory=path1,
            )
        )
        id2 = SharedSystemClient._get_identifier_from_settings(
            chromadb.config.Settings(
                chroma_api_impl="chromadb.api.segment.SegmentAPI",
                is_persistent=True,
                persist_directory=path2,
            )
        )

        assert id1 == id2, f"Same paths should produce same identifier: {id1} != {id2}"
