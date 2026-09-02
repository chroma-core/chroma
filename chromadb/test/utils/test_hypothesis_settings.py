import pytest

import chromadb.test.conftest as test_config


@pytest.mark.parametrize(
    ("preset", "mode", "expected"),
    [
        ("fast", "auto", False),
        ("normal", "auto", False),
        ("slow", "auto", True),
        ("normal", "always", True),
        ("slow", "never", False),
        ("normal", "ALWAYS", True),
    ],
)
def test_hypothesis_driven_compaction_waits_in_cluster(
    monkeypatch: pytest.MonkeyPatch,
    preset: str,
    mode: str,
    expected: bool,
) -> None:
    monkeypatch.setattr(test_config, "NOT_CLUSTER_ONLY", False)
    monkeypatch.setattr(test_config, "CURRENT_PRESET", preset)
    monkeypatch.setenv(test_config.HYPOTHESIS_COMPACTION_WAITS_ENV, mode)

    assert test_config.hypothesis_driven_compaction_waits_enabled() is expected


def test_hypothesis_driven_compaction_waits_are_disabled_outside_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(test_config, "NOT_CLUSTER_ONLY", True)
    monkeypatch.setenv(test_config.HYPOTHESIS_COMPACTION_WAITS_ENV, "always")

    assert test_config.hypothesis_driven_compaction_waits_enabled() is False


def test_hypothesis_driven_compaction_waits_reject_invalid_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(test_config, "NOT_CLUSTER_ONLY", False)
    monkeypatch.setenv(test_config.HYPOTHESIS_COMPACTION_WAITS_ENV, "sometimes")

    with pytest.raises(ValueError, match="Expected one of: auto, always, never"):
        test_config.hypothesis_driven_compaction_waits_enabled()
