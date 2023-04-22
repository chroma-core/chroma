from typing import Literal, Sequence, Union, cast
from chromadb.test.property.strategies import RecordSet
import numpy as np
from chromadb.api import API, types
from chromadb.api.models.Collection import Collection
from hypothesis import note
from hypothesis.errors import InvalidArgument


def count(api: API, collection_name: str, expected_count: int):
    """The given collection count is equal to the number of embeddings"""
    count = api._count(collection_name)
    assert count == expected_count


def _field_matches(
    collection: Collection,
    embeddings: RecordSet,
    field_name: Union[Literal["documents"], Literal["metadatas"]],
):
    """
    The actual embedding field is equal to the expected field
    field_name: one of [documents, metadatas]
    """
    result = collection.get(ids=embeddings["ids"], include=[field_name])
    # TODO: The returned data is not necessarily in the same order as the input ids
    # until we add a sort in the get path
    # The test_out_of_order_ids test fails because of this in test_add.py
    # Here we sort by the ids to match the input order
    embedding_id_to_index = {id: i for i, id in enumerate(embeddings["ids"])}
    actual_field = result[field_name]
    # This assert should never happen, if we include metadatas/documents it will be
    # [None, None..] if there is no metadata. It will not be just None.
    assert actual_field is not None
    actual_field = sorted(
        enumerate(actual_field),
        key=lambda index_and_field_value: embedding_id_to_index[
            result["ids"][index_and_field_value[0]]
        ],
    )
    actual_field = [field_value for _, field_value in actual_field]

    expected_field = embeddings[field_name]
    if expected_field is None:
        # Since an RecordSet is the user input, we need to convert the documents to
        # a List since thats what the API returns -> none per entry
        expected_field = [None] * len(embeddings["ids"])
    assert actual_field == expected_field


def ids_match(collection: Collection, embeddings: RecordSet):
    """The actual embedding ids is equal to the expected ids"""
    actual_ids = collection.get(ids=embeddings["ids"], include=[])["ids"]
    # TODO: The returned ids are not necessarily in the same order as the input ids
    # until we add a sort.
    # The test_out_of_order_ids test fails because of this in test_add.py
    # Here we sort the ids to match the input order
    embedding_id_to_index = {id: i for i, id in enumerate(embeddings["ids"])}
    actual_ids = sorted(actual_ids, key=lambda id: embedding_id_to_index[id])
    assert actual_ids == embeddings["ids"]


def metadatas_match(collection: Collection, embeddings: RecordSet):
    """The actual embedding metadata is equal to the expected metadata"""
    _field_matches(collection, embeddings, "metadatas")


def documents_match(collection: Collection, embeddings: RecordSet):
    """The actual embedding documents is equal to the expected documents"""
    _field_matches(collection, embeddings, "documents")


def no_duplicates(collection: Collection):
    ids = collection.get()["ids"]
    assert len(ids) == len(set(ids))


def ann_accuracy(
    collection: Collection,
    embeddings: RecordSet,
    min_recall: float = 0.99,
):
    """Validate that the API performs nearest_neighbor searches correctly"""

    if len(embeddings["ids"]) == 0:
        return  # nothing to test here

    # Validate that each embedding is its own nearest neighbor and adjust recall if not.
    result = collection.query(
        query_embeddings=embeddings["embeddings"],
        query_texts=embeddings["documents"]
        if embeddings["embeddings"] is None
        else None,
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )

    missing = 0
    for i, id in enumerate(embeddings["ids"]):
        if result["ids"][i][0] != id:
            missing += 1
        else:
            if embeddings["embeddings"] is not None:
                assert np.allclose(
                    result["embeddings"][i][0], embeddings["embeddings"][i]
                )
            assert result["documents"][i][0] == (
                embeddings["documents"][i]
                if embeddings["documents"] is not None
                else None
            )
            assert result["metadatas"][i][0] == (
                embeddings["metadatas"][i]
                if embeddings["metadatas"] is not None
                else None
            )
            assert result["distances"][i][0] == 0.0

    size = len(embeddings["ids"])
    recall = (size - missing) / size

    try:
        note(f"recall: {recall}, missing {missing} out of {size}")
    except InvalidArgument:
        pass  # it's ok if we're running outside hypothesis

    assert recall >= min_recall
