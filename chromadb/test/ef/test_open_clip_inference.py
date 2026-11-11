import numpy as np
import pytest

from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction


def test_open_clip_image_embeddings_use_eval_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    open_clip = pytest.importorskip("open_clip")
    pytest.importorskip("torch")
    pytest.importorskip("PIL")
    # A real, small ResNet CLIP model avoids downloading pretrained weights.
    monkeypatch.setitem(
        open_clip.factory._MODEL_CONFIGS,
        "chroma-test-resnet",
        {
            "embed_dim": 8,
            "vision_cfg": {"layers": [1, 1, 1, 1], "width": 8, "image_size": 32},
            "text_cfg": {
                "context_length": 4,
                "vocab_size": 49408,
                "width": 8,
                "heads": 1,
                "layers": 1,
            },
        },
    )
    ef = OpenCLIPEmbeddingFunction(model_name="chroma-test-resnet", checkpoint="")
    image = np.full((32, 32, 3), 128, dtype=np.uint8)

    first = np.asarray(ef([image])[0])
    second = np.asarray(ef([image])[0])

    assert not ef._model.training
    assert first.shape == (8,)
    assert np.isfinite(first).all()
    np.testing.assert_allclose(np.linalg.norm(first), 1.0, atol=1e-6)
    np.testing.assert_allclose(first, second)
