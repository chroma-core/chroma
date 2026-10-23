from types import SimpleNamespace

import pytest

import chromadb.test.utils.wait_for_version_increase as wait_module


class _FakeClient:
    def __init__(self, collection_id: str = "test-collection-id") -> None:
        self._collection = SimpleNamespace(id=collection_id)

    def get_collection(self, collection_name: str) -> SimpleNamespace:
        return self._collection


def test_wait_for_version_increase_logs_target_version(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    client = _FakeClient()
    versions = iter([5, 5, 6])
    times = iter([100.0, 101.0, 102.0])

    monkeypatch.setattr(wait_module, "COMPACTION_SLEEP", 10)
    monkeypatch.setattr(wait_module, "get_collection_version", lambda *_: next(versions))
    monkeypatch.setattr(wait_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(wait_module.time, "time", lambda: next(times))

    new_version = wait_module.wait_for_version_increase(client, "test-collection", 5)

    assert new_version == 6
    assert (
        "[wait_for_version_increase] collection=test-collection "
        "waiting for version >= 6 (current=5, timeout=10s)"
    ) in capsys.readouterr().out


def test_wait_for_version_increase_timeout_mentions_waited_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    versions = iter([8, 8])
    times = iter([200.0, 212.0])

    monkeypatch.setattr(wait_module, "COMPACTION_SLEEP", 10)
    monkeypatch.setattr(wait_module, "get_collection_version", lambda *_: next(versions))
    monkeypatch.setattr(wait_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(wait_module.time, "time", lambda: next(times))

    with pytest.raises(
        TimeoutError,
        match="waited for version >= 9, last seen version 8",
    ):
        wait_module.wait_for_version_increase(client, "test-collection", 8)
