from typing import (
    TYPE_CHECKING,
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
    Loadable,
    Metadata,
    Metadatas,
    Document,
    Documents,
    Image,
    Images,
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
    validate_metadatas,
    validate_embeddings,
    validate_embedding_function,
)

# TODO: We should rename the types in chromadb.types to be Models where
# appropriate. This will help to distinguish between manipulation objects
# which are essentially API views. And the actual data models which are
# stored / retrieved / transmitted.
from chromadb.types import Collection as CollectionModel
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
        return self._model["id"]

    @property
    def name(self) -> str:
        return self._model["name"]

    @property
    def metadata(self) -> CollectionMetadata:
        return cast(CollectionMetadata, self._model["metadata"])

    @property
    def tenant(self) -> str:
        return self._model["tenant"]

    @property
    def database(self) -> str:
        return self._model["database"]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CollectionCommon):
            return False
        id_match = self.id == other.id
        name_match = self.name == other.name
        metadata_match = self.metadata == other.metadata
        tenant_match = self.tenant == other.tenant
        database_match = self.database == other.database
        embedding_function_match = self._embedding_function == other._embedding_function
        data_loader_match = self._data_loader == other._data_loader
        return (
            id_match
            and name_match
            and metadata_match
            and tenant_match
            and database_match
            and embedding_function_match
            and data_loader_match
        )

    def get_model(self) -> CollectionModel:
        return self._model

    def _validate_embedding_set(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
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

    @staticmethod
    def _normalize_embeddings(
        embeddings: Union[
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
