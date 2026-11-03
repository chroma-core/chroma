import sys
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest

from chromadb.utils.embedding_functions.open_clip_embedding_function import (
    OpenCLIPEmbeddingFunction,
)


@pytest.fixture
def open_clip_ef(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Build the EF with mocked dependencies so no model weights are downloaded."""
    mock_modules = {
        "open_clip": MagicMock(),
        "torch": MagicMock(),
        "PIL": MagicMock(),
        "PIL.Image": MagicMock(),
    }
    mock_modules["open_clip"].create_model_and_transforms.return_value = (
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )
    for name, module in mock_modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    ef = OpenCLIPEmbeddingFunction()

    # Replace the model-backed encoders with deterministic stubs
    monkeypatch.setattr(
        ef, "_encode_text", lambda text: np.array([0.1, 0.2, 0.3], dtype=np.float32)
    )
    monkeypatch.setattr(
        ef, "_encode_image", lambda image: np.array([0.4, 0.5, 0.6], dtype=np.float32)
    )
    return ef


def test_open_clip_ef_rejects_unsupported_input_types(open_clip_ef: Any) -> None:
    """Unsupported input items must raise instead of being silently skipped."""
    ef = open_clip_ef
    image = np.zeros((2, 2, 3), dtype=np.int64)

    # A valid document or image still produces one embedding each
    assert len(ef(["hello world"])) == 1
    assert len(ef([image])) == 1
    assert len(ef(["hello world", image])) == 2

    # A mixed input list must not silently drop the unsupported item
    with pytest.raises(ValueError, match="int"):
        ef(["hello world", 123])

    with pytest.raises(ValueError, match="NoneType"):
        ef(["hello world", None])
