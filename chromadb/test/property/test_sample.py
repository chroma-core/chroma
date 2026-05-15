import uuid
from typing import Any, Dict, List, Sequence, Set, cast

import pytest

from chromadb.api import ClientAPI
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings, Metadatas
from chromadb.test.conftest import (
    create_isolated_database,
    is_spann_disabled_mode,
    skip_reason_spann_disabled,
)
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase
from chromadb.utils.batch_utils import create_batches


PHRASE_EMBEDDINGS: Dict[str, List[float]] = {
    "alpine observatory": [100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "brass foundry": [0.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "cedar archive": [0.0, 0.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "delta harbor": [0.0, 0.0, 0.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "ember market": [0.0, 0.0, 0.0, 0.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "frost garden": [0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 0.0, 0.0, 0.0, 0.0],
    "granite library": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 0.0, 0.0, 0.0],
    "hazel workshop": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 0.0, 0.0],
    "indigo station": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 0.0],
    "jade theater": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0],
}

RECORDS_PER_PHRASE = 1000
TOTAL_RECORDS = len(PHRASE_EMBEDDINGS) * RECORDS_PER_PHRASE
SAMPLE_LIMIT = 200


class LocalPhraseEmbeddingModel(EmbeddingFunction[Documents]):
    def __init__(self) -> None:
        pass

    def __call__(self, input: Documents) -> Embeddings:
        return [PHRASE_EMBEDDINGS[document] for document in input]

    @staticmethod
    def name() -> str:
        return "local_phrase_embedding_model"

    def get_config(self) -> Dict[str, Any]:
        return {}

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "LocalPhraseEmbeddingModel":
        return LocalPhraseEmbeddingModel()


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_stratified_sample_covers_all_phrase_keys_after_compaction(
    client: ClientAPI,
) -> None:
    if (
        client.get_settings().chroma_api_impl
        == "chromadb.api.async_fastapi.AsyncFastAPI"
    ):
        pytest.skip(
            "TODO @robert, come back and debug why CI runners fail with async + sync"
        )
    create_isolated_database(client)

    model = LocalPhraseEmbeddingModel()
    collection_name = f"sample_strata_{uuid.uuid4().hex}"
    collection = client.create_collection(
        name=collection_name,
        embedding_function=model,
        configuration={
            "spann": {
                "space": "l2",
                "search_nprobe": 10,
                "write_nprobe": 1,
                "split_threshold": 200,
                "merge_threshold": 12,
                "ef_construction": 100,
                "ef_search": 100,
                "max_neighbors": 16,
            }
        },
    )
    initial_version = cast(int, collection.get_model()["version"])

    ids: List[str] = []
    documents: List[str] = []
    metadatas: Metadatas = []
    for phrase in PHRASE_EMBEDDINGS:
        for repeat in range(RECORDS_PER_PHRASE):
            ids.append(f"{phrase.replace(' ', '-')}-{repeat}")
            documents.append(phrase)
            metadatas.append({"phrase_key": phrase, "repeat": repeat})

    embeddings = cast(Embeddings, model(cast(Documents, documents)))
    for batch in create_batches(
        api=client,
        ids=ids,
        embeddings=embeddings,
        metadatas=metadatas,
        documents=documents,
    ):
        collection.add(*batch)

    assert collection.count() == TOTAL_RECORDS
    wait_for_version_increase(client, collection.name, initial_version, additional_time=300)

    sample = collection.sample(
        limit=SAMPLE_LIMIT,
        seed=13,
        include=["metadatas", "documents"],
    )

    sampled_metadatas = sample["metadatas"]
    assert sampled_metadatas is not None
    sampled_keys: Set[str] = {
        cast(str, metadata["phrase_key"])
        for metadata in cast(Sequence[Dict[str, object]], sampled_metadatas)
        if metadata is not None
    }
    assert sampled_keys == set(PHRASE_EMBEDDINGS)
