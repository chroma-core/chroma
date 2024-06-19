import pytest
import numpy
from chromadb.utils.embedding_functions import SpacyEmbeddingFunction

input_list = ["great work by the guy", "Super man is that guy"]
model_name = "en_core_web_md"
unknown_model = "unknown_model"
spacy = pytest.importorskip("spacy", reason="spacy not installed")


def test_spacyembeddingfunction_isnotnone_wheninputisnotnone():
    spacy_emb_fn = SpacyEmbeddingFunction(model_name)
    assert spacy_emb_fn(input_list) is not None


def test_spacyembddingfunction_throwserror_whenmodel_notfound():
    with pytest.raises(
        ValueError,
        match=r"""spacy models are not downloaded yet, please download them using `spacy download model_name`, Please checkout
                for the list of models from: https://spacy.io/usage/models.""",
    ):
        SpacyEmbeddingFunction(unknown_model)


def test_spacyembddingfunction_isembedding_wheninput_islist():
    spacy_emb_fn = SpacyEmbeddingFunction(model_name)
    assert type(spacy_emb_fn(input_list)) is list


def test_spacyembeddingfunction_returnslistoflistsofloats():
    spacy_emb_fn = SpacyEmbeddingFunction(model_name)
    expected_output = spacy_emb_fn(input_list)
    assert type(expected_output[0]) is list
    assert type(expected_output[0][0]) is numpy.float64
