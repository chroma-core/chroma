import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import numpy as np
import pytest

from chromadb.utils.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)


def test_ollama_default_model() -> None:
    pytest.importorskip("ollama", reason="ollama not installed")
    ef = OllamaEmbeddingFunction()
    embeddings = ef(["Here is an article about llamas...", "this is another article"])
    assert embeddings is not None
    assert len(embeddings) == 2
    assert all(len(e) == 384 for e in embeddings)


def test_ollama_unknown_model() -> None:
    pytest.importorskip("ollama", reason="ollama not installed")
    model_name = "unknown-model"
    ef = OllamaEmbeddingFunction(model_name=model_name)
    with pytest.raises(Exception) as e:
        ef(["Here is an article about llamas...", "this is another article"])
    assert f'model "{model_name}" not found' in str(e.value)


def test_ollama_backward_compat() -> None:
    pytest.importorskip("ollama", reason="ollama not installed")
    ef = OllamaEmbeddingFunction(url="http://localhost:11434/api/embeddings")
    embeddings = ef(["Here is an article about llamas...", "this is another article"])
    assert embeddings is not None


def test_wrong_url() -> None:
    pytest.importorskip("ollama", reason="ollama not installed")
    ef = OllamaEmbeddingFunction(url="http://localhost:11434/this_is_wrong")
    with pytest.raises(Exception) as e:
        ef(["Here is an article about llamas...", "this is another article"])
    assert "404" in str(e.value)


def test_ollama_ask_user_to_install() -> None:
    try:
        from ollama import Client  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("ollama python package is installed")
    with pytest.raises(ValueError) as e:
        OllamaEmbeddingFunction()
    assert "The ollama python package is not installed" in str(e.value)


@pytest.mark.parametrize("prefix", ["", "/ollama", "/gateway/tenant/ollama"])
@pytest.mark.parametrize("legacy_endpoint", [False, True])
def test_ollama_url_path(
    prefix: str, legacy_endpoint: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("ollama", reason="ollama not installed")
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    paths = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            paths.append(self.path)
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path != prefix + "/api/embed":
                self.send_error(404)
                return
            assert body["input"] == ["first", "second"]
            assert body["model"] == "test-model"
            data = json.dumps({"embeddings": [[1.0, 0.0], [0.0, 1.0]]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}{prefix}"
            if legacy_endpoint:
                url += "/api/embeddings"
            ef = OllamaEmbeddingFunction(url=url, model_name="test-model")
            restored = OllamaEmbeddingFunction.build_from_config(ef.get_config())
            for function in [ef, restored]:
                embeddings = function(["first", "second"])
                np.testing.assert_array_equal(embeddings, [[1.0, 0.0], [0.0, 1.0]])
            assert paths == [prefix + "/api/embed"] * 2
        finally:
            server.shutdown()
            thread.join()
