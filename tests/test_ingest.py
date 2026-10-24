from typing import Any, Dict, List
import chromadb
import numpy as np
import pandas as pd
import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from chromadb.api.models.AsyncCollection import AsyncCollection
from chromadb.types import Collection as CollectionModel


@pytest.fixture
def col():
    client = chromadb.EphemeralClient()
    c = client.get_or_create_collection("test_ingest_native", embedding_function=None)
    yield c
    client.delete_collection("test_ingest_native")


def df5():
    return pd.DataFrame({
        "id": list("abcde"),
        "text": ["one", "two", None, "four", "five"],
        "year": [2020, 2021, np.nan, 2023, 2024],
        "score": [1.5, np.nan, 3.5, 4.5, 5.5],
        "when": pd.to_datetime(["2024-01-01", None, "2024-03-01", "2024-04-01", "2024-05-01"]),
        "emb": [[float(i), 1.0, 0.0] for i in range(5)],
    })


def test_dataframe_nan_and_timestamps(col):
    n = col.add_data(df5(), document_cols="text", id_col="id", embedding_col="emb")
    assert n == 5
    got = col.get(ids=["b", "c"], include=["metadatas", "documents"])
    by_id = dict(zip(got["ids"], got["metadatas"]))
    assert "score" not in by_id["b"]            # NaN dropped
    assert "year" not in by_id["c"]
    assert "when" not in by_id["b"] or by_id["b"]["when"] is not None
    assert by_id["c"]["when"].startswith("2024-03-01")


def test_batching(col):
    col.add_data(df5(), document_cols="text", id_col="id", embedding_col="emb", batch_size=2)
    assert col.count() == 5


def test_multi_column_documents(col):
    col.add_data(df5(), document_cols=["text", "year"], id_col="id",
                 embedding_col="emb", metadata_cols=["score"])
    doc = col.get(ids=["a"])["documents"][0]
    assert doc == "text: one | year: 2020.0"


def test_list_of_dicts(col):
    rows = [
        {"id": "x", "text": "hello", "tags": ["a", "b"], "emb": [1, 0, 0]},
        {"id": "y", "text": "world", "emb": [0, 1, 0]},       # missing key is fine
    ]
    col.add_data(rows, document_cols="text", id_col="id", embedding_col="emb")
    assert col.get(ids=["x"])["metadatas"][0]["tags"] == '["a", "b"]'


def test_dict_of_columns(col):
    col.add_data({"id": ["p", "q"], "text": ["t1", "t2"], "emb": [[1, 0, 0], [0, 1, 0]]},
                 document_cols="text", id_col="id", embedding_col="emb")
    assert col.count() == 2


def test_single_record(col):
    col.add_data({"id": "solo", "text": "just one", "emb": [1, 0, 0]},
                 document_cols="text", id_col="id", embedding_col="emb")
    assert col.count() == 1


def test_chroma_native_dict(col):
    col.add_data({"ids": ["n1"], "documents": ["d"], "embeddings": [[1, 0, 0]],
                  "metadatas": [{"k": np.int64(3)}]})
    assert col.get(ids=["n1"])["metadatas"][0]["k"] == 3


def test_upsert_hash_ids_are_idempotent(col):
    rows = [{"text": "same", "emb": [1, 0, 0]}]
    col.upsert_data(rows, document_cols="text", embedding_col="emb", id_strategy="hash")
    col.upsert_data(rows, document_cols="text", embedding_col="emb", id_strategy="hash")
    assert col.count() == 1


def test_add_overload_dataframe(col):
    col.add(df5(), document_cols="text", id_col="id", embedding_col="emb")
    assert col.count() == 5


def test_add_overload_does_not_break_normal_use(col):
    col.add(ids=["z"], embeddings=[[1, 0, 0]], documents=["ok"])
    assert col.count() == 1


def test_duplicate_ids_rejected(col):
    with pytest.raises(ValueError, match="duplicate"):
        col.add_data([{"id": "a", "t": "x"}, {"id": "a", "t": "y"}], document_cols="t", id_col="id")


def test_bad_column_message(col):
    with pytest.raises(KeyError, match="Available"):
        col.add_data(df5(), document_cols="nope", id_col="id")


def test_aliases(col):
    df = pd.DataFrame({"id": ["al1"], "text": ["t1"], "emb": [[1, 0, 0]]})
    col.add_dataframe(df, document_cols="text", id_col="id", embedding_col="emb")
    assert col.count() == 1

    records = [{"id": "al2", "text": "t2", "emb": [0, 1, 0]}]
    col.add_records(records, document_cols="text", id_col="id", embedding_col="emb")
    assert col.count() == 2

    col.upsert_dataframe(df, document_cols="text", id_col="id", embedding_col="emb")
    col.upsert_records(records, document_cols="text", id_col="id", embedding_col="emb")
    assert col.count() == 2


def test_get_dataframe(col):
    col.add_data(
        pd.DataFrame({"id": ["g1", "g2"], "text": ["hello", "world"], "emb": [[1, 0, 0], [0, 1, 0]], "val": [10, 20]}),
        document_cols="text",
        id_col="id",
        embedding_col="emb",
    )
    result_df = col.get_dataframe()
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert "id" in result_df.columns
    assert "document" in result_df.columns
    assert "val" in result_df.columns


@pytest.mark.asyncio
async def test_async_collection_ingest():
    client_mock = MagicMock()
    client_mock.get_max_batch_size = MagicMock(return_value=100)
    client_mock._add = AsyncMock()
    client_mock._upsert = AsyncMock()

    model = CollectionModel(
        id=uuid4(),
        name="test_async_coll",
        configuration_json={},
        serialized_schema=None,
        metadata=None,
        dimension=3,
        tenant="default_tenant",
        database="default_database",
    )
    async_col = AsyncCollection(client=client_mock, model=model, embedding_function=None)

    # test async add_data
    rows = [{"id": "1", "text": "hello", "emb": [0.1, 0.2, 0.3]}]
    n = await async_col.add_data(rows, document_cols="text", id_col="id", embedding_col="emb")
    assert n == 1
    assert client_mock._add.called

    # test async upsert_data
    n_up = await async_col.upsert_data(rows, document_cols="text", id_col="id", embedding_col="emb")
    assert n_up == 1
    assert client_mock._upsert.called

    # test async add overload
    client_mock._add.reset_mock()
    await async_col.add(rows, document_cols="text", id_col="id", embedding_col="emb")
    assert client_mock._add.called
