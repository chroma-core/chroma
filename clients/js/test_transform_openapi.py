import importlib.util
from pathlib import Path


def _load_transform_openapi():
    module_path = Path(__file__).with_name("transform-openapi.py")
    spec = importlib.util.spec_from_file_location("transform_openapi", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_fetch_openapi_json_uses_timeout(monkeypatch):
    module = _load_transform_openapi()
    calls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"openapi": "3.0.0"}'

    def fake_urlopen(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    assert module.fetch_openapi_json("http://localhost:8000/openapi.json") == {
        "openapi": "3.0.0"
    }
    assert calls == [
        (
            "http://localhost:8000/openapi.json",
            {"timeout": module.OPENAPI_FETCH_TIMEOUT_SECONDS},
        )
    ]
