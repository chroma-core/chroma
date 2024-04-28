import pytest
from chromadb.utils.embedding_functions import SpacyEmbeddingFunction

input_list = ["great work by the guy", "Super man is that guy"]
model_name = "md"
unknown_model = "unknown_model"


def test_spacyembeddingfunction_isnotnone_wheninputisnotnone():
    spacy_emb_fn = SpacyEmbeddingFunction(model_name)
    assert spacy_emb_fn(input_list) is not None


def test_spacyembddingfunction_throwserror_whenmodel_notfound():
    with pytest.raises(ValueError,
                       match='spacy models are not downloaded, '
                             'please download them using '
                             '`spacy download en-core-web-lg or en-core-web-md`'):
        SpacyEmbeddingFunction(unknown_model)


def test_spacyembddingfunction_isembedding_wheninput_islist():
    spacy_emb_fn = SpacyEmbeddingFunction(model_name)
    assert type(spacy_emb_fn(input_list)) is list
