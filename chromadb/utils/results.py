from typing import List, Dict, Any, cast
import pandas as pd
import numpy as np
from chromadb.api.types import QueryResult, GetResult, GetOrQueryResult


def _query_result_to_dfs(query_result: QueryResult) -> List["pd.DataFrame"]:
    """Helper function to convert QueryResult to list of DataFrames."""
    included_fields = query_result["included"]
    dfs = []
    num_queries = len(query_result["ids"])

    for i in range(num_queries):
        data_for_df: Dict[str, Any] = {}

        data_for_df["id"] = query_result["ids"][i]

        if "documents" in included_fields and query_result["documents"] is not None:
            data_for_df["document"] = query_result["documents"][i]

        if "uris" in included_fields and query_result["uris"] is not None:
            data_for_df["uri"] = query_result["uris"][i]

        if "data" in included_fields and query_result["data"] is not None:
            data_for_df["data"] = query_result["data"][i]

        if "metadatas" in included_fields and query_result["metadatas"] is not None:
            data_for_df["metadata"] = query_result["metadatas"][i]

        if "distances" in included_fields and query_result["distances"] is not None:
            data_for_df["distance"] = query_result["distances"][i]

        if "embeddings" in included_fields and query_result["embeddings"] is not None:
            embeddings_list = query_result["embeddings"][i]
            if embeddings_list and isinstance(embeddings_list[0], np.ndarray):
                data_for_df["embedding"] = [emb.tolist() for emb in embeddings_list]  # type: ignore
            else:
                data_for_df["embedding"] = embeddings_list

        # Create DataFrame
        df = pd.DataFrame(data_for_df)

        df.set_index("id", inplace=True)

        dfs.append(df)
    return dfs


def _get_result_to_df(get_result: GetResult) -> "pd.DataFrame":
    """Helper function to convert GetResult to a DataFrame."""
    included_fields = get_result["included"]
    data_for_df: Dict[str, Any] = {}

    data_for_df["id"] = get_result["ids"]

    if "documents" in included_fields and get_result["documents"] is not None:
        data_for_df["document"] = get_result["documents"]

    if "uris" in included_fields and get_result["uris"] is not None:
        data_for_df["uri"] = get_result["uris"]

    if "data" in included_fields and get_result["data"] is not None:
        data_for_df["data"] = get_result["data"]

    if "metadatas" in included_fields and get_result["metadatas"] is not None:
        data_for_df["metadata"] = get_result["metadatas"]

    if "embeddings" in included_fields and get_result["embeddings"] is not None:
        embeddings_list = get_result["embeddings"]
        if embeddings_list and isinstance(embeddings_list[0], np.ndarray):
            data_for_df["embedding"] = [emb.tolist() for emb in embeddings_list]  # type: ignore
        else:
            data_for_df["embedding"] = embeddings_list

    # Create a single DataFrame
    df = pd.DataFrame(data_for_df)
    df.set_index("id", inplace=True)

    return df


def results_to_dfs(result: GetOrQueryResult) -> List["pd.DataFrame"]:
    """
    Converts a QueryResult or GetResult dictionary to a list of pandas DataFrames.

    Delegates to helper functions based on the structure of the input.
    Columns are included based on the 'included' field in the result.
    Column order is id, document, uri, data, metadata, distance (if applicable), embedding.
    """
    # IDs type differs between QueryResult and GetResult, use that to determine type
    # GetResult will have a flat list of ids, QueryResult will have a list of lists of ids
    ids = result.get("ids")
    is_query_result = isinstance(ids, list) and bool(ids) and isinstance(ids[0], list)

    if not ids:
        return []

    if is_query_result:
        return _query_result_to_dfs(cast(QueryResult, result))
    else:
        return [_get_result_to_df(cast(GetResult, result))]
