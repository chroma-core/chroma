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
    Embeddable,
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
    validate_embeddings,
    validate_embedding_function,
    validate_n_results,
    validate_where,
    validate_where_document,
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

    def _validate_embedding_set(
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
        require_embeddings_or_data: bool = True,
    ) -> Tuple[
        IDs,
        Optional[Embeddings],
        Optional[Metadatas],
        Optional[Documents],
        Optional[Images],
        Optional[URIs],
    ]:
        valid_ids = validate_ids(maybe_cast_one_to_many_ids(ids))
        valid_embeddings = (
            validate_embeddings(
                self._normalize_embeddings(maybe_cast_one_to_many_embedding(embeddings))
            )
            if embeddings is not None
            else None
        )
        valid_metadatas = (
            validate_metadatas(maybe_cast_one_to_many_metadata(metadatas))
            if metadatas is not None
            else None
        )
        valid_documents = (
            maybe_cast_one_to_many_document(documents)
            if documents is not None
            else None
        )
        valid_images = (
            maybe_cast_one_to_many_image(images) if images is not None else None
        )

        valid_uris = maybe_cast_one_to_many_uri(uris) if uris is not None else None

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

        return (
            valid_ids,
            valid_embeddings,
            valid_metadatas,
            valid_documents,
            valid_images,
            valid_uris,
        )

    def _validate_and_prepare_embedding_set(
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
    ) -> Tuple[
        IDs,
        Embeddings,
        Optional[Metadatas],
        Optional[Documents],
        Optional[URIs],
    ]:
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

        return ids, embeddings, metadatas, documents, uris

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
        valid_ids = validate_ids(maybe_cast_one_to_many_ids(ids)) if ids else None
        valid_include = validate_include(include, allow_distances=False)

        if "data" in include and self._data_loader is None:
            raise ValueError(
                "You must set a data loader on the collection if loading from URIs."
            )

        # We need to include uris in the result from the API to load datas
        if "data" in include and "uris" not in include:
            valid_include.append("uris")  # type: ignore[arg-type]

        return valid_ids, valid_where, valid_where_document, valid_include

    def _transform_get_response(
        self, response: GetResult, include: Include
    ) -> GetResult:
        if (
            "data" in include
            and self._data_loader is not None
            and response["uris"] is not None
        ):
            response["data"] = self._data_loader(response["uris"])

        # Remove URIs from the result if they weren't requested
        if "uris" not in include:
            response["uris"] = None

        return response

    def _validate_and_prepare_query_request(
        self,
        query_embeddings: Optional[  # type: ignore[type-arg]
            Union[
                OneOrMany[Embedding],
                OneOrMany[np.ndarray],
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

    def _validate_and_prepare_update_request(
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
    ) -> Tuple[
        IDs,
        Embeddings,
        Optional[Metadatas],
        Optional[Documents],
        Optional[URIs],
    ]:
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

        return ids, cast(Embeddings, embeddings), metadatas, documents, uris

    def _validate_and_prepare_upsert_request(
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
    ) -> Tuple[
        IDs,
        Embeddings,
        Optional[Metadatas],
        Optional[Documents],
        Optional[URIs],
    ]:
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

        return ids, embeddings, metadatas, documents, uris

    def _validate_and_prepare_delete_request(
        self,
        ids: Optional[IDs],
        where: Optional[Where],
        where_document: Optional[WhereDocument],
    ) -> Tuple[Optional[IDs], Optional[Where], Optional[WhereDocument]]:
        ids = validate_ids(maybe_cast_one_to_many_ids(ids)) if ids else None
        where = validate_where(where) if where else None
        where_document = (
            validate_where_document(where_document) if where_document else None
        )

        return (ids, where, where_document)

    @staticmethod
    def _normalize_embeddings(
        embeddings: Union[  # type: ignore[type-arg]
            OneOrMany[Embedding],
            OneOrMany[np.ndarray],
        ]
    ) -> Embeddings:
        if isinstance(embeddings, np.ndarray):
            return embeddings.tolist()  # type: ignore
        return embeddings  # type: ignore

    def _embed(self, input: Any) -> Embeddings:
        if self._embedding_function is None:
            raise ValueError(
                "You must provide an embedding function to compute embeddings."
                "https://docs.trychroma.com/guides/embeddings"
            )
        return self._embedding_function(input=input)
