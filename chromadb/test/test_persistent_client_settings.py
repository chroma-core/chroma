from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest

import chromadb
from chromadb.api.client import Client
from chromadb.config import Settings


@pytest.mark.parametrize("use_explicit_path", [True, False])
def test_persistent_client_does_not_mutate_supplied_settings(
    tmp_path: Path, use_explicit_path: bool
) -> None:
    settings = Settings(
        is_persistent=False,
        persist_directory="configured_path",
        allow_reset=True,
        chroma_memory_limit_bytes=8192,
    )
    original = settings.model_dump()

    with patch("chromadb.ClientCreator") as create_client:
        if use_explicit_path:
            chromadb.PersistentClient(path=tmp_path, settings=settings)
        else:
            chromadb.PersistentClient(settings=settings)

    client_settings = create_client.call_args.kwargs["settings"]
    assert settings.model_dump() == original
    assert client_settings is not settings
    assert client_settings.model_dump() == dict(
        original,
        persist_directory=str(tmp_path) if use_explicit_path else "./chroma",
        is_persistent=True,
    )


@pytest.mark.parametrize(
    "api_impl",
    ["chromadb.api.segment.SegmentAPI", "chromadb.api.rust.RustBindingsAPI"],
)
def test_persistent_clients_can_reuse_settings(tmp_path: Path, api_impl: str) -> None:
    if api_impl == "chromadb.api.rust.RustBindingsAPI":
        pytest.importorskip("chromadb_rust_bindings")

    settings = Settings(chroma_api_impl=api_impl, anonymized_telemetry=False)
    first_path, second_path = tmp_path / "first", tmp_path / "second"

    with cast(
        Client, chromadb.PersistentClient(path=first_path, settings=settings)
    ) as first:
        with cast(
            Client, chromadb.PersistentClient(path=second_path, settings=settings)
        ) as second:
            first_collection = first.create_collection(
                "records", embedding_function=None
            )
            first_collection.add(ids=["first"], embeddings=[[1.0, 0.0]])
            second_collection = second.create_collection(
                "records", embedding_function=None
            )
            second_collection.add(ids=["second"], embeddings=[[0.0, 1.0]])

            # A new handle must still recognize the first client's settings after
            # the same settings template has been used for a different path.
            with cast(
                Client,
                chromadb.PersistentClient(
                    path=first_path,
                    settings=Settings(
                        chroma_api_impl=api_impl, anonymized_telemetry=False
                    ),
                ),
            ) as reopened:
                assert reopened.get_collection(
                    "records", embedding_function=None
                ).get()["ids"] == ["first"]
                assert first.get_settings().persist_directory == str(first_path)
                assert second.get_settings().persist_directory == str(second_path)

        # Closing the second client must leave the first one usable.
        assert first_collection.query(query_embeddings=[[1.0, 0.0]], n_results=1)[
            "ids"
        ] == [["first"]]

    # Both stores must remain readable after their original systems have closed.
    for path, expected_id in [(first_path, "first"), (second_path, "second")]:
        with cast(
            Client, chromadb.PersistentClient(path=path, settings=settings)
        ) as client:
            assert client.get_collection("records", embedding_function=None).get()[
                "ids"
            ] == [expected_id]
