from typing import Optional, Tuple, List
from chromadb.api import BaseAPI
from chromadb.api.types import (
    Documents,
    Embeddings,
    IDs,
    Metadatas,
)


def create_batches(
    api: BaseAPI,
    ids: IDs,
    embeddings: Optional[Embeddings] = None,
    metadatas: Optional[Metadatas] = None,
    documents: Optional[Documents] = None,
    max_batch_size: Optional[int] = None,
) -> List[Tuple[IDs, Optional[Embeddings], Optional[Metadatas], Optional[Documents]]]:
    _batches: List[
        Tuple[IDs, Optional[Embeddings], Optional[Metadatas], Optional[Documents]]
    ] = []
    server_max_batch_size = api.get_max_batch_size()
    if max_batch_size is None:
        max_batch_size = server_max_batch_size
    else:
        if max_batch_size > server_max_batch_size:
            max_batch_size = server_max_batch_size

    if len(ids) > max_batch_size:
        # create split batches
        for i in range(0, len(ids), max_batch_size):
            _batches.append(
                (
                    ids[i : i + max_batch_size],
                    embeddings[i : i + max_batch_size]
                    if embeddings is not None
                    else None,
                    metadatas[i : i + max_batch_size] if metadatas else None,
                    documents[i : i + max_batch_size] if documents else None,
                )
            )
    else:
        _batches.append((ids, embeddings, metadatas, documents))
    return _batches
