from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
)
import numpy as np

from chromadb.api.types import (
    URI,
    CollectionMetadata,
    Embedding,
    Include,
    Metadata,
    Document,
    Image,
    Where,
    IDs,
    GetResult,
    QueryResult,
    ID,
    OneOrMany,
    WhereDocument,
    maybe_cast_one_to_many_ids,
    maybe_cast_one_to_many_embedding,
    maybe_cast_one_to_many_document,
    maybe_cast_one_to_many_image,
    maybe_cast_one_to_many_uri,
    validate_ids,
    validate_include,
    validate_metadata,
    validate_where,
    validate_where_document,
    validate_n_results,
    validate_embeddings,
)

from .CollectionCommon import CollectionCommon

if TYPE_CHECKING:
    from chromadb.api import ServerAPIAsync


class CollectionAsync(CollectionCommon["ServerAPIAsync"]):
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
    ) -> None:
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

        await self._client._add(ids, self.id, embeddings, metadatas, documents, uris)

    async def count(self) -> int:
        return await self._client._count(collection_id=self.id)

    async def get(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        where: Optional[Where] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents"],
    ) -> GetResult:
        valid_where = validate_where(where) if where else None
        valid_where_document = (
            validate_where_document(where_document) if where_document else None
        )
        valid_ids = validate_ids(maybe_cast_one_to_many_ids(ids)) if ids else None
        valid_include = validate_include(include, allow_distances=False)

        if "data" in include and self._data_loader is None:
            raise ValueError(
                "You must set a data loader on the collection if loading from URIs."
            )

        # We need to include uris in the result from the API to load datas
        if "data" in include and "uris" not in include:
            valid_include.append("uris")

        get_results = await self._client._get(
            self.id,
            valid_ids,
            valid_where,
            None,
            limit,
            offset,
            where_document=valid_where_document,
            include=valid_include,
        )

        if (
            "data" in include
            and self._data_loader is not None
            and get_results["uris"] is not None
        ):
            get_results["data"] = self._data_loader(get_results["uris"])

        # Remove URIs from the result if they weren't requested
        if "uris" not in include:
            get_results["uris"] = None

        return get_results

    async def peek(self, limit: int = 10) -> GetResult:
        return await self._client._peek(self.id, limit)

    async def query(
        self,
        query_embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[np.ndarray],
            ]
        ] = None,
        query_texts: Optional[OneOrMany[Document]] = None,
        query_images: Optional[OneOrMany[Image]] = None,
        query_uris: Optional[OneOrMany[URI]] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents", "distances"],
    ) -> QueryResult:
        # Users must provide only one of query_embeddings, query_texts, query_images, or query_uris
        if not (
            (query_embeddings is not None)
            ^ (query_texts is not None)
            ^ (query_images is not None)
            ^ (query_uris is not None)
        ):
            raise ValueError(
                "You must provide one of query_embeddings, query_texts, query_images, or query_uris."
            )

        valid_where = validate_where(where) if where else {}
        valid_where_document = (
            validate_where_document(where_document) if where_document else {}
        )
        valid_query_embeddings = (
            validate_embeddings(
                self._normalize_embeddings(
                    maybe_cast_one_to_many_embedding(query_embeddings)
                )
            )
            if query_embeddings is not None
            else None
        )
        valid_query_texts = (
            maybe_cast_one_to_many_document(query_texts)
            if query_texts is not None
            else None
        )
        valid_query_images = (
            maybe_cast_one_to_many_image(query_images)
            if query_images is not None
            else None
        )
        valid_query_uris = (
            maybe_cast_one_to_many_uri(query_uris) if query_uris is not None else None
        )
        valid_include = validate_include(include, allow_distances=True)
        valid_n_results = validate_n_results(n_results)

        # If query_embeddings are not provided, we need to compute them from the inputs
        if valid_query_embeddings is None:
            if query_texts is not None:
                valid_query_embeddings = self._embed(input=valid_query_texts)
            elif query_images is not None:
                valid_query_embeddings = self._embed(input=valid_query_images)
            else:
                if valid_query_uris is None:
                    raise ValueError(
                        "You must provide either query_embeddings, query_texts, query_images, or query_uris."
                    )
                if self._data_loader is None:
                    raise ValueError(
                        "You must set a data loader on the collection if loading from URIs."
                    )
                valid_query_embeddings = self._embed(
                    self._data_loader(valid_query_uris)
                )

        if "data" in include and "uris" not in include:
            valid_include.append("uris")
        query_results = await self._client._query(
            collection_id=self.id,
            query_embeddings=valid_query_embeddings,
            n_results=valid_n_results,
            where=valid_where,
            where_document=valid_where_document,
            include=include,
        )

        if (
            "data" in include
            and self._data_loader is not None
            and query_results["uris"] is not None
        ):
            query_results["data"] = [
                self._data_loader(uris) for uris in query_results["uris"]
            ]

        # Remove URIs from the result if they weren't requested
        if "uris" not in include:
            query_results["uris"] = None

        return query_results

    async def modify(
        self, name: Optional[str] = None, metadata: Optional[CollectionMetadata] = None
    ) -> None:
        if metadata is not None:
            validate_metadata(metadata)
            if "hnsw:space" in metadata:
                raise ValueError(
                    "Changing the distance function of a collection once it is created is not supported currently."
                )

        # Note there is a race condition here where the metadata can be updated
        # but another thread sees the cached local metadata.
        # TODO: fixme
        await self._client._modify(id=self.id, new_name=name, new_metadata=metadata)
        if name:
            self._model["name"] = name
        if metadata:
            self._model["metadata"] = metadata

    async def update(
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
    ) -> None:
        (
            ids,
            embeddings,
            metadatas,
            documents,
            images,
            uris,
        ) = self._validate_embedding_set(
            ids,
            embeddings,
            metadatas,
            documents,
            images,
            uris,
            require_embeddings_or_data=False,
        )

        if embeddings is None:
            if documents is not None:
                embeddings = self._embed(input=documents)
            elif images is not None:
                embeddings = self._embed(input=images)

        await self._client._update(self.id, ids, embeddings, metadatas, documents, uris)

    async def upsert(
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
    ) -> None:
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

        if embeddings is None:
            if documents is not None:
                embeddings = self._embed(input=documents)
            else:
                embeddings = self._embed(input=images)

        await self._client._upsert(
            collection_id=self.id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
        )

    async def delete(
        self,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
    ) -> None:
        ids = validate_ids(maybe_cast_one_to_many_ids(ids)) if ids else None
        where = validate_where(where) if where else None
        where_document = (
            validate_where_document(where_document) if where_document else None
        )

        await self._client._delete(self.id, ids, where, where_document)
