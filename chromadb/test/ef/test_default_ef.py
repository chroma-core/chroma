import hashlib
import io
import shutil
import os
import tarfile
from pathlib import Path
from typing import List, Hashable

import hypothesis.strategies as st
import onnxruntime
import pytest
from hypothesis import given, settings
from onnxruntime.datasets import get_example
from unittest.mock import patch, MagicMock

from chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 import (
    ONNXMiniLM_L6_V2,
)
from chromadb.api.types import DefaultEmbeddingFunction

from chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 import _verify_sha256


def unique_by(x: Hashable) -> Hashable:
    return x


ONNX_FILES = [
    "config.json",
    "model.onnx",
    "special_tokens_map.json",
    "tokenizer_config.json",
    "tokenizer.json",
    "vocab.txt",
]


def _write_tiny_onnx_archive(path: Path) -> str:
    model_bytes = Path(get_example("mul_1.onnx")).read_bytes()
    archive_files = {
        "config.json": b"{}",
        "model.onnx": model_bytes,
        "special_tokens_map.json": b"{}",
        "tokenizer_config.json": b"{}",
        "tokenizer.json": b"{}",
        "vocab.txt": b"",
    }

    with tarfile.open(path, "w:gz") as tar:
        for filename, contents in archive_files.items():
            tarinfo = tarfile.TarInfo(f"onnx/{filename}")
            tarinfo.size = len(contents)
            tar.addfile(tarinfo, io.BytesIO(contents))

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _use_tiny_model_download(
    ef: ONNXMiniLM_L6_V2, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> str:
    archive = tmp_path / "onnx.tar.gz"
    expected_sha256 = _write_tiny_onnx_archive(archive)
    download_path = tmp_path / "model-cache"
    monkeypatch.setattr(ef, "DOWNLOAD_PATH", download_path)

    def download(url: str, fname: str, chunk_size: int = 1024) -> None:
        shutil.copyfile(archive, fname)
        if not _verify_sha256(fname, ef._MODEL_SHA256):
            os.remove(fname)
            raise ValueError(
                f"Downloaded file {fname} does not match expected SHA256 hash. Corrupted download or malicious file."
            )

    monkeypatch.setattr(ef, "_download", download)
    return expected_sha256


@settings(deadline=None)
@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_all_providers()).filter(
            lambda x: x not in onnxruntime.get_available_providers()
        ),
        unique_by=unique_by,
        min_size=1,
    )
)
def test_unavailable_provider_multiple(providers: List[str]) -> None:
    with pytest.raises(ValueError) as e:
        ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
        ef(["test"])
    assert "Preferred providers must be subset of available providers" in str(e.value)


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_available_providers()),
        min_size=1,
        unique_by=unique_by,
    )
)
def test_available_provider(providers: List[str]) -> None:
    ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
    ef(["test"])


def test_warning_no_providers_supplied() -> None:
    ef = ONNXMiniLM_L6_V2()
    ef(["test"])


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_available_providers()),
        min_size=1,
    ).filter(lambda x: len(x) > len(set(x)))
)
def test_provider_repeating(providers: List[str]) -> None:
    with pytest.raises(ValueError) as e:
        ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
        ef(["test"])
    assert "Preferred providers must be unique" in str(e.value)


def test_invalid_sha256(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ef = ONNXMiniLM_L6_V2()
    _use_tiny_model_download(ef, tmp_path, monkeypatch)
    with pytest.raises(ValueError) as e:
        ef._MODEL_SHA256 = "invalid"
        ef._download_model_if_not_exists()
    assert "does not match expected SHA256 hash" in str(e.value)


def test_partial_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ef = ONNXMiniLM_L6_V2()
    ef._MODEL_SHA256 = _use_tiny_model_download(ef, tmp_path, monkeypatch)
    os.makedirs(ef.DOWNLOAD_PATH, exist_ok=True)
    path = os.path.join(ef.DOWNLOAD_PATH, ef.ARCHIVE_FILENAME)
    with open(path, "wb") as f:  # create invalid file to simulate partial download
        f.write(b"invalid")
    ef._download_model_if_not_exists()  # re-download model
    assert os.path.exists(path)
    assert _verify_sha256(
        str(os.path.join(ef.DOWNLOAD_PATH, ef.ARCHIVE_FILENAME)),
        ef._MODEL_SHA256,
    )
    for filename in ONNX_FILES:
        assert os.path.exists(
            os.path.join(ef.DOWNLOAD_PATH, ef.EXTRACTED_FOLDER_NAME, filename)
        )


def test_singleton_default_ef_is_reused() -> None:
    """DefaultEmbeddingFunction must reuse a single ONNXMiniLM_L6_V2 instance."""
    DefaultEmbeddingFunction._singleton = None  # reset for isolation
    with patch(
        "chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2.ONNXMiniLM_L6_V2"
    ) as mock_ef:
        mock_instance = mock_ef.return_value
        mock_instance.return_value = [[0.5, 0.5]]
        ef = DefaultEmbeddingFunction()
        ef(["doc1"])
        ef(["doc2"])
        mock_ef.assert_called_once()


def test_default_ef_pins_threads_on_session() -> None:
    """The default EF should construct ONNXMiniLM_L6_V2 with single-threaded ops."""
    DefaultEmbeddingFunction._singleton = None  # reset for isolation
    with patch(
        "chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2.ONNXMiniLM_L6_V2"
    ) as mock_ef:
        mock_instance = mock_ef.return_value
        mock_instance.return_value = [[0.5, 0.5]]
        DefaultEmbeddingFunction()(["doc"])
        _, kwargs = mock_ef.call_args
        assert kwargs["intra_op_num_threads"] == 1
        assert kwargs["inter_op_num_threads"] == 1


def test_session_options_thread_counts() -> None:
    """ONNXMiniLM_L6_V2 must propagate intra/inter-op thread counts to SessionOptions."""
    ef = ONNXMiniLM_L6_V2()
    ef._intra_op_num_threads = 2
    ef._inter_op_num_threads = 3

    with patch.object(
        ef.ort, "InferenceSession", return_value=MagicMock()
    ) as mock_session:
        with patch.object(ef, "_preferred_providers", ["CPUExecutionProvider"]):
            ef.model
        sess_options = mock_session.call_args.kwargs["sess_options"]
    assert sess_options.intra_op_num_threads == 2
    assert sess_options.inter_op_num_threads == 3
