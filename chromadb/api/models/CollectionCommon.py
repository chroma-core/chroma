from typing import (
    TYPE_CHECKING,
    Dict,
    Generic,
    Optional,
    Tuple,
    Any,
    TypeVar,
    Union,
    cast,
)
import numpy as np
from uuid import UUID

import chromadb.utils.embedding_functions as ef
from chromadb.api.types import (
    URI,
    CollectionMetadata,
    DataLoader,
    Embedding,
    Embeddings,
    PyEmbedding,
    Embeddable,
    RecordSet,
    GetResult,
    Include,
    Loadable,
    Metadata,
    Metadatas,
    Document,
    Documents,
    Image,
    Images,
    QueryResult,
    URIs,
    IDs,
    EmbeddingFunction,
    ID,
    OneOrMany,
    maybe_cast_one_to_many,
    maybe_cast_one_to_many_embedding,
    validate_ids,
    validate_include,
    validate_metadata,
    validate_metadatas,
    validate_embeddings,
    validate_embedding_function,
    validate_n_results,
    validate_where,
    validate_where_document,
    does_record_set_contain_data,
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

    def _unpack_record_set(
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
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> RecordSet:
        unpacked_ids = maybe_cast_one_to_many(ids)
        unpacked_embeddings = maybe_cast_one_to_many_embedding(embeddings)
        unpacked_metadatas = maybe_cast_one_to_many(metadatas)
        unpacked_documents = maybe_cast_one_to_many(documents)
        unpacked_images = maybe_cast_one_to_many(images)
        unpacked_uris = maybe_cast_one_to_many(uris)
        return {
            "ids": cast(IDs, unpacked_ids),
            "embeddings": unpacked_embeddings,
            "metadatas": unpacked_metadatas,
            "documents": unpacked_documents,
            "images": unpacked_images,
            "uris": unpacked_uris,
        }

    def _validate_record_set(
        self,
        ids: IDs,
        embeddings: Optional[Embeddings],
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
        images: Optional[Images],
        uris: Optional[URIs],
        require_embeddings_or_data: bool = True,
    ) -> None:
        valid_ids = validate_ids(ids)
        valid_embeddings = (
            validate_embeddings(embeddings) if embeddings is not None else None
        )
        valid_metadatas = (
            validate_metadatas(metadatas) if metadatas is not None else None
        )

        # No additional validation needed for documents, images, or uris
        valid_documents = documents
        valid_images = images
        valid_uris = uris

        # Check that one of embeddings or ducuments or images is provided
        if require_embeddings_or_data:
            if (
                valid_embeddings is None
                and valid_documents is None
                and valid_images is None
                and valid_uris is None
            ):
                raise ValueError(
                    "You must provide embeddings, documents, images, or uris."
                )

        # Only one of documents or images can be provided
        if valid_documents is not None and valid_images is not None:
            raise ValueError("You can only provide documents or images, not both.")

        # Check that, if they're provided, the lengths of the arrays match the length of ids
        if valid_embeddings is not None and len(valid_embeddings) != len(valid_ids):
            raise ValueError(
                f"Number of embeddings {len(valid_embeddings)} must match number of ids {len(valid_ids)}"
            )
        if valid_metadatas is not None and len(valid_metadatas) != len(valid_ids):
            raise ValueError(
                f"Number of metadatas {len(valid_metadatas)} must match number of ids {len(valid_ids)}"
            )
        if valid_documents is not None and len(valid_documents) != len(valid_ids):
            raise ValueError(
                f"Number of documents {len(valid_documents)} must match number of ids {len(valid_ids)}"
            )
        if valid_images is not None and len(valid_images) != len(valid_ids):
            raise ValueError(
                f"Number of images {len(valid_images)} must match number of ids {len(valid_ids)}"
            )
        if valid_uris is not None and len(valid_uris) != len(valid_ids):
            raise ValueError(
                f"Number of uris {len(valid_uris)} must match number of ids {len(valid_ids)}"
            )

    def _compute_embeddings(
        self,
        documents: Optional[Documents],
        images: Optional[Images],
        uris: Optional[URIs],
    ) -> Embeddings:
        # We need to compute the embeddings if they're not provided

        embeddings = None
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

        return embeddings

    def _validate_and_prepare_get_request(
        self,
        ids: Optional[OneOrMany[ID]],
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        include: Include,
    ) -> Tuple[Optional[IDs], Optional[Where], Optional[WhereDocument], Include,]:
        valid_where = validate_where(where) if where else None
        valid_where_document = (
            validate_where_document(where_document) if where_document else None
        )
        valid_ids = (
            validate_ids(cast(IDs, maybe_cast_one_to_many(ids))) if ids else None
        )
        valid_include = validate_include(include, allow_distances=False)

        if "data" in include and self._data_loader is None:
            raise ValueError(
                "You must set a data loader on the collection if loading from URIs."
            )

        # We need to include uris in the result from the API to load datas
        if "data" in include and "uris" not in include:
            valid_include.append("uris")  # type: ignore[arg-type]

        return valid_ids, valid_where, valid_where_document, valid_include

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
    ) -> Tuple[Embeddings, int, Where, WhereDocument,]:
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
                    cast(Embeddings, maybe_cast_one_to_many_embedding(query_embeddings))
                )
            )
            if query_embeddings is not None
            else None
        )
        valid_query_texts = (
            maybe_cast_one_to_many(query_texts) if query_texts is not None else None
        )
        valid_query_images = (
            maybe_cast_one_to_many(query_images) if query_images is not None else None
        )
        valid_query_uris = (
            maybe_cast_one_to_many(query_uris) if query_uris is not None else None
        )
        valid_include = validate_include(include, allow_distances=True)
        valid_n_results = validate_n_results(n_results)

        # If query_embeddings are not provided, we need to compute them from the inputs
        valid_query_embeddings = (
            self._compute_embeddings(
                documents=valid_query_texts,
                images=valid_query_images,
                uris=valid_query_uris,
            )
            if valid_query_embeddings is None
            else valid_query_embeddings
        )

        if "data" in include and "uris" not in include:
            valid_include.append("uris")  # type: ignore[arg-type]

        return (
            valid_query_embeddings,
            valid_n_results,
            valid_where,
            valid_where_document,
        )

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

    def _process_add_request(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[  # type: ignore[type-arg]
            Union[
                OneOrMany[Embedding],
                OneOrMany[np.ndarray],
            ]
        ],
        metadatas: Optional[OneOrMany[Metadata]],
        documents: Optional[OneOrMany[Document]],
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> RecordSet:
        unpacked_embedding_set = self._unpack_record_set(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        normalized_embeddings = (
            self._normalize_embeddings(unpacked_embedding_set["embeddings"])
            if unpacked_embedding_set["embeddings"] is not None
            else None
        )

        self._validate_record_set(
            ids=unpacked_embedding_set["ids"],
            embeddings=normalized_embeddings,
            metadatas=unpacked_embedding_set["metadatas"],
            documents=unpacked_embedding_set["documents"],
            images=unpacked_embedding_set["images"],
            uris=unpacked_embedding_set["uris"],
        )

        prepared_embeddings = (
            self._compute_embeddings(
                documents=unpacked_embedding_set["documents"],
                images=unpacked_embedding_set["images"],
                uris=unpacked_embedding_set["uris"],
            )
            if normalized_embeddings is None
            else normalized_embeddings
        )

        return {
            "ids": unpacked_embedding_set["ids"],
            "embeddings": prepared_embeddings,
            "metadatas": unpacked_embedding_set["metadatas"],
            "documents": unpacked_embedding_set["documents"],
            "images": unpacked_embedding_set["images"],
            "uris": unpacked_embedding_set["uris"],
        }

    def _process_upsert_request(
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
    ) -> RecordSet:
        unpacked_record_set = self._unpack_record_set(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        normalized_embeddings = (
            self._normalize_embeddings(unpacked_record_set["embeddings"])
            if unpacked_record_set["embeddings"] is not None
            else None
        )

        self._validate_record_set(
            ids=unpacked_record_set["ids"],
            embeddings=normalized_embeddings,
            metadatas=unpacked_record_set["metadatas"],
            documents=unpacked_record_set["documents"],
            images=unpacked_record_set["images"],
            uris=unpacked_record_set["uris"],
        )

        prepared_embeddings = (
            self._compute_embeddings(
                documents=unpacked_record_set["documents"],
                images=unpacked_record_set["images"],
                uris=None,
            )
            if normalized_embeddings is None
            else normalized_embeddings
        )

        return {
            "ids": unpacked_record_set["ids"],
            "embeddings": prepared_embeddings,
            "metadatas": unpacked_record_set["metadatas"],
            "documents": unpacked_record_set["documents"],
            "images": unpacked_record_set["images"],
            "uris": unpacked_record_set["uris"],
        }

    def _process_update_request(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[  # type: ignore[type-arg]
            Union[
                OneOrMany[Embedding],
                OneOrMany[np.ndarray],
            ]
        ],
        metadatas: Optional[OneOrMany[Metadata]],
        documents: Optional[OneOrMany[Document]],
        images: Optional[OneOrMany[Image]],
        uris: Optional[OneOrMany[URI]],
    ) -> RecordSet:
        unpacked_record_set = self._unpack_record_set(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        normalized_embeddings = (
            self._normalize_embeddings(unpacked_record_set["embeddings"])
            if unpacked_record_set["embeddings"] is not None
            else None
        )

        self._validate_record_set(
            ids=unpacked_record_set["ids"],
            embeddings=normalized_embeddings,
            metadatas=unpacked_record_set["metadatas"],
            documents=unpacked_record_set["documents"],
            images=unpacked_record_set["images"],
            uris=unpacked_record_set["uris"],
            require_embeddings_or_data=False,
        )

        prepared_embeddings = normalized_embeddings
        if does_record_set_contain_data(
            unpacked_record_set, ignore=["ids", "metadatas", "embeddings"]
        ):
            prepared_embeddings = self._compute_embeddings(
                documents=unpacked_record_set["documents"],
                images=unpacked_record_set["images"],
                uris=None,
            )

        return {
            "ids": unpacked_record_set["ids"],
            "embeddings": prepared_embeddings,
            "metadatas": unpacked_record_set["metadatas"],
            "documents": unpacked_record_set["documents"],
            "images": unpacked_record_set["images"],
            "uris": unpacked_record_set["uris"],
        }

    def _validate_and_prepare_delete_request(
        self,
        ids: Optional[IDs],
        where: Optional[Where],
        where_document: Optional[WhereDocument],
    ) -> Tuple[Optional[IDs], Optional[Where], Optional[WhereDocument]]:
        ids = validate_ids(cast(IDs, maybe_cast_one_to_many(ids))) if ids else None
        where = validate_where(where) if where else None
        where_document = (
            validate_where_document(where_document) if where_document else None
        )

        return (ids, where, where_document)

    @staticmethod
    def _normalize_embeddings(
        embeddings: Union[
            OneOrMany[Embedding],
            OneOrMany[PyEmbedding],
        ]
    ) -> Embeddings:
        return cast(Embeddings, [np.array(embedding) for embedding in embeddings])

    def _embed(self, input: Any) -> Embeddings:
        if self._embedding_function is None:
            raise ValueError(
                "You must provide an embedding function to compute embeddings."
                "https://docs.trychroma.com/guides/embeddings"
            )
        return self._embedding_function(input=input)
