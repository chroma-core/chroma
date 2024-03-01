from chromadb.utils.embedding_functions import TransformerEmbeddingFunction


def test_transformer_ef_default_mdoel():
    ef = TransformerEmbeddingFunction()
    embedding = ef(["text"])
    assert len(embedding[0]) == 384


def test_transformer_ef_custom_model():
    ef = TransformerEmbeddingFunction(model_name="dbmdz/bert-base-turkish-cased")
    embedding = ef(["Merhaba dünya", "Bu bir test cümlesidir"])
    assert embedding is not None
    assert len(embedding) == 2
    assert len(embedding[0]) == 768
