import chromadb


def test_resolve_version_uses_installed_distribution_version(monkeypatch):
    monkeypatch.setattr(chromadb, "metadata_version", lambda _: "1.2.3.dev15")
    assert chromadb._resolve_version() == "1.2.3.dev15"


def test_resolve_version_falls_back_when_distribution_metadata_missing(monkeypatch):
    def _raise_package_not_found(_: str) -> str:
        raise chromadb.PackageNotFoundError

    monkeypatch.setattr(chromadb, "metadata_version", _raise_package_not_found)
    assert chromadb._resolve_version() == chromadb._STATIC_VERSION
