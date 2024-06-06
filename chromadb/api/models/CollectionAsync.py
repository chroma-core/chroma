# todo: cleanup imports
from typing import TYPE_CHECKING, Optional, Tuple, Any, Union, cast
import numpy as np
from uuid import UUID

from overrides import overrides
import chromadb.utils.embedding_functions as ef
from chromadb.api.types import (
    URI,
    CollectionMetadata,
    DataLoader,
    Embedding,
    Embeddings,
    Embeddable,
    Include,
    Loadable,
    Metadata,
    Metadatas,
    Document,
    Documents,
    Image,
    Images,
    URIs,
    Where,
    IDs,
    EmbeddingFunction,
    GetResult,
    QueryResult,
    ID,
    OneOrMany,
    WhereDocument,
    maybe_cast_one_to_many_ids,
    maybe_cast_one_to_many_embedding,
    maybe_cast_one_to_many_metadata,
    maybe_cast_one_to_many_document,
    maybe_cast_one_to_many_image,
    maybe_cast_one_to_many_uri,
    validate_ids,
    validate_include,
    validate_metadata,
    validate_metadatas,
    validate_where,
    validate_where_document,
    validate_n_results,
    validate_embeddings,
    validate_embedding_function,
)

from .CollectionCommon import CollectionCommon


class CollectionAsync(CollectionCommon):
    @overrides
    async def add(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[np.ndarray],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ):
        (
            ids,
            embeddings,
            metadatas,
            documents,
            images,
            uris,
        ) = self._validate_embedding_set(
            ids, embeddings, metadatas, documents, images, uris
        )

        # We need to compute the embeddings if they're not provided
        if embeddings is None:
            # At this point, we know that one of documents or images are provided from the validation above
            if documents is not None:
                embeddings = self._embed(input=documents)
            elif images is not None:
                embeddings = self._embed(input=images)
            else:
                if uris is None:
                    raise ValueError(
                        "You must provide either embeddings, documents, images, or uris."
                    )
                if self._data_loader is None:
                    raise ValueError(
                        "You must set a data loader on the collection if loading from URIs."
                    )
                embeddings = self._embed(self._data_loader(uris))

        # todo: fix type
        await self._client._add(ids, self.id, embeddings, metadatas, documents, uris)

    @overrides
    async def count(self) -> int:
        return await self._client._count(collection_id=self.id)
