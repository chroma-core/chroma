import pytest
from chromadb.utils.embedding_functions import SpacyEmbeddingFunction

input_list = ["great work by the guy", "Super man is that guy"]
model_name = "en_core_web_md"
unknown_model = "unknown_model"


def test_spacyembeddingfunction_isnotnone_wheninputisnotnone():
    spacy_emb_fn = SpacyEmbeddingFunction(model_name)
    assert spacy_emb_fn(input_list) is not None


def test_spacyembddingfunction_throwserror_whenmodel_notfound():
    with pytest.raises(ValueError,
                       match="""spacy models are not downloaded yet, please download them using `spacy download model_name`, Please checkout
                for the list of models from: https://spacy.io/usage/models. By default the module will load en_core_web_lg
                model as it optimizes accuracy and has embeddings in-built, please download and load with `en_core_web_md` 
                if you want to priortize efficiency over accuracy, the same logic applies for models from other languages also.
                language_web_core_sm and language_web_core_trf doesn't have pre-trained embeddings."""):
        SpacyEmbeddingFunction(unknown_model)


def test_spacyembddingfunction_isembedding_wheninput_islist():
    spacy_emb_fn = SpacyEmbeddingFunction(model_name)
    assert type(spacy_emb_fn(input_list)) is list
