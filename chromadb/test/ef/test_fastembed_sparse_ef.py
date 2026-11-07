from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest

from chromadb.utils.embedding_functions.fastembed_sparse_embedding_function import (
    FastembedSparseEmbeddingFunction,
)

splade_pp = pytest.importorskip(
    "fastembed.sparse.splade_pp", reason="fastembed not installed"
)


@pytest.mark.parametrize(
    "options",
    [
        {"providers": ["CPUExecutionProvider"]},
        {"cuda": True, "device_ids": [2, 3]},
    ],
)
@pytest.mark.parametrize("lazy_load", [True, False])
@pytest.mark.parametrize("from_config", [True, False])
def test_fastembed_sparse_model_options(
    tmp_path: Path,
    options: Dict[str, Any],
    lazy_load: bool,
    from_config: bool,
) -> None:
    # Keep the real FastEmbed constructors, but avoid downloads and ONNX/GPU setup.
    with patch.object(
        splade_pp.SpladePP, "download_model", return_value=tmp_path
    ), patch.object(splade_pp.SpladePP, "load_onnx_model") as load_model:
        ef = FastembedSparseEmbeddingFunction(
            model_name="prithivida/Splade_PP_en_v1",
            cache_dir=str(tmp_path),
            threads=2,
            lazy_load=lazy_load,
            **options,
        )
        if from_config:
            config = ef.get_config()
            load_model.reset_mock()
            restored = FastembedSparseEmbeddingFunction.build_from_config(config)
            assert isinstance(restored, FastembedSparseEmbeddingFunction)
            ef = restored
            assert ef.get_config() == config

        model = ef._model.model
        assert model.providers == options.get("providers")
        assert model.cuda == options.get("cuda")
        assert model.device_ids == options.get("device_ids")
        assert model.lazy_load is lazy_load
        assert model.threads == 2
        assert model.cache_dir == str(tmp_path)
        if lazy_load:
            load_model.assert_not_called()
        else:
            load_model.assert_called_once()
