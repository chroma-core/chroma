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
) -> List[Tuple[IDs, Embeddings, Optional[Metadatas], Optional[Documents]]]:
    _batches: List[
        Tuple[IDs, Embeddings, Optional[Metadatas], Optional[Documents]]
    ] = []
    max_batch_size = api.get_max_batch_size()
    offset = 0
    if len(ids) > max_batch_size:
        while offset < len(ids):
            batch_size = random.randint(1, max_batch_size):
            _batches.append(
                (  # type: ignore
                    ids[offset : offset + batch_size],
                    embeddings[offset : offset + batch_size]
                    if embeddings
                    else None,
                    metadatas[offset : offset + batch_size] if metadatas else None,
                    documents[offset : offset + batch_size] if documents else None,
                )
            )
            offset += batch_size
    else:
        _batches.append((ids, embeddings, metadatas, documents))  # type: ignore
    return _batches
