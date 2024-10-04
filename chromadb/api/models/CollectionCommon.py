import functools
from dataclasses import asdict
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Generic,
    Optional,
    Any,
    TypeVar,
    Union,
    cast,
)
from chromadb.types import Metadata
import numpy as np
from uuid import UUID

import chromadb.utils.embedding_functions as ef
from chromadb.api.types import (
    URI,
    AddRequest,
    CollectionMetadata,
    DataLoader,
    DeleteRequest,
    Embedding,
    Embeddings,
    FilterSet,
    GetRequest,
    IncludeEnum,
    PyEmbedding,
    Embeddable,
    GetResult,
    Include,
    Loadable,
    Document,
    Image,
    QueryRequest,
    QueryResult,
    IDs,
    EmbeddingFunction,
    ID,
    OneOrMany,
    UpdateRequest,
    UpsertRequest,
    maybe_cast_one_to_many,
    validate_ids,
    validate_metadata,
    validate_embedding_function,
    validate_where,
    validate_where_document,
    RecordSet,
)

# TODO: We should rename the types in chromadb.types to be Models where
# appropriate. This will help to distinguish between manipulation objects
# which are essentially API views. And the actual data models which are
# stored / retrieved / transmitted.
from chromadb.types import Collection as CollectionModel, Where, WhereDocument
import logging

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from chromadb.api import ServerAPI, AsyncServerAPI

ClientT = TypeVar("ClientT", "ServerAPI", "AsyncServerAPI")

T = TypeVar("T")


def validation_context(name: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> T:
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                msg = f"{str(e)} in {name}."
                raise type(e)(msg).with_traceback(e.__traceback__)

        return wrapper

    return decorator


class CollectionCommon(Generic[ClientT]):
    _model: CollectionModel
    _client: ClientT
    _embedding_function: Optional[EmbeddingFunction[Embeddable]]
    _data_loader: Optional[DataLoader[Loadable]]

    def __init__(
        self,
        client: ClientT,
        model: CollectionModel,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
    ):
        """Initializes a new instance of the Collection class."""

        self._client = client
        self._model = model

        # Check to make sure the embedding function has the right signature, as defined by the EmbeddingFunction protocol
        if embedding_function is not None:
            validate_embedding_function(embedding_function)

        self._embedding_function = embedding_function
        self._data_loader = data_loader

    # Expose the model properties as read-only properties on the Collection class

    @property
    def id(self) -> UUID:
        return self._model.id

    @property
    def name(self) -> str:
        return self._model.name

    @property
    def configuration_json(self) -> Dict[str, Any]:
        return self._model.configuration_json

    @property
    def metadata(self) -> CollectionMetadata:
        return cast(CollectionMetadata, self._model.metadata)

    @property
    def tenant(self) -> str:
        return self._model.tenant

    @property
    def database(self) -> str:
        return self._model.database

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CollectionCommon):
            return False
        id_match = self.id == other.id
        name_match = self.name == other.name
        configuration_match = self.configuration_json == other.configuration_json
        metadata_match = self.metadata == other.metadata
        tenant_match = self.tenant == other.tenant
        database_match = self.database == other.database
        embedding_function_match = self._embedding_function == other._embedding_function
        data_loader_match = self._data_loader == other._data_loader
        return (
            id_match
            and name_match
            and configuration_match
            and metadata_match
            and tenant_match
            and database_match
            and embedding_function_match
            and data_loader_match
        )

    def __repr__(self) -> str:
        return f"Collection(id={self.id}, name={self.name})"

    def get_model(self) -> CollectionModel:
        return self._model

    @validation_context("add")
    def _validate_and_prepare_add_request(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ],
        metadatas: Optional[OneOrMany[Metadata]],
        documents: Optional[OneOrMany[Document]],
        images: Optional[OneOrMany[Image]],
        uris: Optional[OneOrMany[URI]],
    ) -> AddRequest:
        # Unpack
        add_records = RecordSet.unpack(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        # Validate
        add_records.validate()
        add_records.validate_contains_any({"ids"})

        # Prepare
        if add_records.embeddings is None:
            add_records.validate_for_embedding()
            add_embeddings = self._embed_record_set(add_records)
        else:
            add_embeddings = add_records.embeddings

        return AddRequest(
            ids=add_records.ids,
            embeddings=add_embeddings,
            metadatas=add_records.metadatas,
            documents=add_records.documents,
            uris=add_records.uris,
        )

    @validation_context("get")
    def _validate_and_prepare_get_request(
        self,
        ids: Optional[OneOrMany[ID]],
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        include: Include,
    ) -> GetRequest:
        # Unpack
        unpacked_ids: Optional[IDs] = maybe_cast_one_to_many(ids)
        filters = FilterSet(where=where, where_document=where_document, include=include)

        # Validate
        if unpacked_ids is not None:
            validate_ids(unpacked_ids)
        filters.validate()

        # Prepare
        if "data" in include and self._data_loader is None:
            raise ValueError(
                "You must set a data loader on the collection if loading from URIs."
            )

        # We need to include uris in the result from the API to load datas
        if "data" in include and "uris" not in include:
            filters.include.append("uris")  # type: ignore[arg-type]

        return GetRequest(
            ids=unpacked_ids,
            where=filters.where,
            where_document=filters.where_document,
            include=filters.include,
        )

    @validation_context("query")
    def _validate_and_prepare_query_request(
        self,
        query_embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ],
        query_texts: Optional[OneOrMany[Document]],
        query_images: Optional[OneOrMany[Image]],
        query_uris: Optional[OneOrMany[URI]],
        n_results: int,
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        include: Include,
    ) -> QueryRequest:
        # Unpack
        query_records = RecordSet.unpack(
            embeddings=query_embeddings,
            documents=query_texts,
            images=query_images,
            uris=query_uris,
        )

        filters = FilterSet(
            where=where,
            where_document=where_document,
            include=include,
            n_results=n_results,
        )

        # Validate
        query_records.validate()
        filters.validate()

        # Prepare
        if query_records.embeddings is None:
            query_records.validate_for_embedding()
            request_embeddings = self._embed_record_set(query_records)
        else:
            request_embeddings = query_records.embeddings

        if filters.where is None:
            request_where = {}
        else:
            request_where = filters.where

        if filters.where_document is None:
            request_where_document = {}
        else:
            request_where_document = filters.where_document

        # We need to manually include uris in the result from the API to load datas
        request_include = filters.include
        if "data" in request_include and "uris" not in request_include:
            request_include.append(IncludeEnum.uris)

        return QueryRequest(
            embeddings=request_embeddings,
            where=request_where,
            where_document=request_where_document,
            include=request_include,
            n_results=cast(int, filters.n_results),
        )

    @validation_context("update")
    def _validate_and_prepare_update_request(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ],
        metadatas: Optional[OneOrMany[Metadata]],
        documents: Optional[OneOrMany[Document]],
        images: Optional[OneOrMany[Image]],
        uris: Optional[OneOrMany[URI]],
    ) -> UpdateRequest:
        # Unpack
        update_records = RecordSet.unpack(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        # Validate
        update_records.validate()
        update_records.validate_contains_any({"ids"})

        # Prepare
        if update_records.embeddings is None:
            # TODO: Handle URI updates.
            if (
                update_records.documents is not None
                or update_records.images is not None
            ):
                update_records.validate_for_embedding(
                    embeddable_fields={"documents", "images"}
                )
                update_embeddings = self._embed_record_set(update_records)
            else:
                update_embeddings = None
        else:
            update_embeddings = update_records.embeddings

        return UpdateRequest(
            ids=update_records.ids,
            embeddings=update_embeddings,
            metadatas=update_records.metadatas,
            documents=update_records.documents,
            uris=update_records.uris,
        )

    @validation_context("upsert")
    def _validate_and_prepare_upsert_request(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> UpsertRequest:
        # Unpack
        upsert_records = RecordSet.unpack(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        # Validate
        upsert_records.validate()
        upsert_records.validate_contains_any({"ids"})

        # Prepare
        if upsert_records.embeddings is None:
            # TODO: Handle URI upserts.
            upsert_records.validate_for_embedding(
                embeddable_fields={"documents", "images"}
            )
            upsert_embeddings = self._embed_record_set(upsert_records)
        else:
            upsert_embeddings = upsert_records.embeddings

        return UpsertRequest(
            ids=upsert_records.ids,
            embeddings=upsert_embeddings,
            metadatas=upsert_records.metadatas,
            documents=upsert_records.documents,
            uris=upsert_records.uris,
        )

    @validation_context("delete")
    def _validate_and_prepare_delete_request(
        self,
        ids: Optional[IDs],
        where: Optional[Where],
        where_document: Optional[WhereDocument],
    ) -> DeleteRequest:
        if ids is None and where is None and where_document is None:
            raise ValueError(
                "At least one of ids, where, or where_document must be provided"
            )

        # Unpack
        if ids is not None:
            request_ids = cast(IDs, maybe_cast_one_to_many(ids))
            validate_ids(request_ids)
        else:
            request_ids = None

        # Validate - Note that FilterSet is not used here since there is no Include or n_results
        if where_document is not None:
            validate_where_document(where_document)
        if where is not None:
            validate_where(where)

        return DeleteRequest(
            ids=request_ids, where=where, where_document=where_document
        )

    def _transform_peek_response(self, response: GetResult) -> GetResult:
        if response["embeddings"] is not None:
            response["embeddings"] = np.array(response["embeddings"])

        return response

    def _transform_get_response(
        self, response: GetResult, include: Include
    ) -> GetResult:
        if (
            "data" in include
            and self._data_loader is not None
            and response["uris"] is not None
        ):
            response["data"] = self._data_loader(response["uris"])

        if "embeddings" in include:
            response["embeddings"] = np.array(response["embeddings"])

        # Remove URIs from the result if they weren't requested
        if "uris" not in include:
            response["uris"] = None

        return response

    def _transform_query_response(
        self, response: QueryResult, include: Include
    ) -> QueryResult:
        if (
            "data" in include
            and self._data_loader is not None
            and response["uris"] is not None
        ):
            response["data"] = [self._data_loader(uris) for uris in response["uris"]]

        if "embeddings" in include and response["embeddings"] is not None:
            response["embeddings"] = [
                np.array(embedding) for embedding in response["embeddings"]
            ]

        # Remove URIs from the result if they weren't requested
        if "uris" not in include:
            response["uris"] = None

        return response

    def _validate_modify_request(self, metadata: Optional[CollectionMetadata]) -> None:
        if metadata is not None:
            validate_metadata(metadata)
            if "hnsw:space" in metadata:
                raise ValueError(
                    "Changing the distance function of a collection once it is created is not supported currently."
                )

    def _update_model_after_modify_success(
        self, name: Optional[str], metadata: Optional[CollectionMetadata]
    ) -> None:
        if name:
            self._model["name"] = name
        if metadata:
            self._model["metadata"] = metadata

    def _embed_record_set(self, record_set: RecordSet) -> Embeddings:
        record_dict = asdict(record_set)
        for field in record_set.get_embeddable_fields():
            if record_dict[field] is not None:
                # uris require special handling
                if field == "uris":
                    if self._data_loader is None:
                        raise ValueError(
                            "You must set a data loader on the collection if loading from URIs."
                        )
                    return self._embed(input=self._data_loader(uris=record_dict[field]))
                else:
                    return self._embed(input=record_dict[field])
        raise ValueError(
            "Record does not contain any fields that can be embedded."
            f"Embeddable Fields: {record_set.get_embeddable_fields()}"
            f"Record Fields: {record_dict.keys()}"
        )

    def _embed(self, input: Any) -> Embeddings:
        if self._embedding_function is None:
            raise ValueError(
                "You must provide an embedding function to compute embeddings."
                "https://docs.trychroma.com/guides/embeddings"
            )
        return self._embedding_function(input=input)
