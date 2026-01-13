import functools
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Generic,
    Optional,
    Any,
    Set,
    TypeVar,
    Union,
    cast,
    List,
)
from chromadb.types import Metadata
import numpy as np
from uuid import UUID

from chromadb.api.types import (
    URI,
    Schema,
    SparseVectorIndexConfig,
    URIs,
    AddRequest,
    BaseRecordSet,
    CollectionMetadata,
    DataLoader,
    DeleteRequest,
    Embedding,
    Embeddings,
    FilterSet,
    GetRequest,
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
    SparseEmbeddingFunction,
    ID,
    OneOrMany,
    UpdateRequest,
    UpsertRequest,
    get_default_embeddable_record_set_fields,
    maybe_cast_one_to_many,
    normalize_base_record_set,
    normalize_insert_record_set,
    validate_base_record_set,
    validate_ids,
    validate_include,
    validate_insert_record_set,
    validate_metadata,
    validate_metadatas,
    validate_embedding_function,
    validate_sparse_embedding_function,
    validate_n_results,
    validate_record_set_contains_any,
    validate_record_set_for_embedding,
    validate_filter_set,
    DefaultEmbeddingFunction,
    EMBEDDING_KEY,
    DOCUMENT_KEY,
)
from chromadb.api.collection_configuration import (
    UpdateCollectionConfiguration,
    overwrite_collection_configuration,
    load_collection_configuration_from_json,
    CollectionConfiguration,
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
    """A decorator that wraps a method with a try-except block that catches
    exceptions and adds the method name to the error message. This allows us to
    provide more context when an error occurs, without rewriting validators.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> T:
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                msg = f"{str(e)} in {name}."
                # add the rest of the args to the error message if they exist
                e.args = (msg,) + e.args[1:] if e.args else ()
                # raise the same error that was caught with the modified message
                raise

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
        ] = DefaultEmbeddingFunction(),  # type: ignore
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
    def configuration(self) -> CollectionConfiguration:
        return load_collection_configuration_from_json(self._model.configuration_json)

    @property
    def configuration_json(self) -> Dict[str, Any]:
        return self._model.configuration_json

    @property
    def schema(self) -> Optional[Schema]:
        return Schema.deserialize_from_json(
            self._model.serialized_schema if self._model.serialized_schema else {}
        )

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
        schema_match = self.schema == other.schema
        metadata_match = self.metadata == other.metadata
        tenant_match = self.tenant == other.tenant
        database_match = self.database == other.database
        embedding_function_match = self._embedding_function == other._embedding_function
        data_loader_match = self._data_loader == other._data_loader
        return (
            id_match
            and name_match
            and configuration_match
            and schema_match
            and metadata_match
            and tenant_match
            and database_match
            and embedding_function_match
            and data_loader_match
        )

    def __repr__(self) -> str:
        return f"Collection(name={self.name})"

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
        add_records = normalize_insert_record_set(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        # Validate
        validate_insert_record_set(record_set=add_records)
        validate_record_set_contains_any(record_set=add_records, contains_any={"ids"})

        # Prepare
        if add_records["embeddings"] is None:
            validate_record_set_for_embedding(record_set=add_records)
            add_embeddings = self._embed_record_set(record_set=add_records)
        else:
            add_embeddings = add_records["embeddings"]

        add_metadatas = self._apply_sparse_embeddings_to_metadatas(
            add_records["metadatas"], add_records["documents"]
        )

        return AddRequest(
            ids=add_records["ids"],
            embeddings=add_embeddings,
            metadatas=add_metadatas,
            documents=add_records["documents"],
            uris=add_records["uris"],
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
        unpacked_ids: Optional[IDs] = maybe_cast_one_to_many(target=ids)
        filters = FilterSet(where=where, where_document=where_document)

        # Validate
        if unpacked_ids is not None:
            validate_ids(ids=unpacked_ids)

        validate_filter_set(filter_set=filters)
        validate_include(include=include, dissalowed=["distances"])

        if "data" in include and self._data_loader is None:
            raise ValueError(
                "You must set a data loader on the collection if loading from URIs."
            )

        # Prepare
        request_include = include
        # We need to include uris in the result from the API to load datas
        if "data" in include and "uris" not in include:
            request_include.append("uris")

        return GetRequest(
            ids=unpacked_ids,
            where=filters["where"],
            where_document=filters["where_document"],
            include=request_include,
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
        ids: Optional[OneOrMany[ID]],
        n_results: int,
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        include: Include,
    ) -> QueryRequest:
        # Unpack
        query_records = normalize_base_record_set(
            embeddings=query_embeddings,
            documents=query_texts,
            images=query_images,
            uris=query_uris,
        )

        filter_ids = maybe_cast_one_to_many(ids)

        filters = FilterSet(
            where=where,
            where_document=where_document,
        )

        # Validate
        validate_base_record_set(record_set=query_records)
        validate_filter_set(filter_set=filters)
        validate_include(include=include)
        validate_n_results(n_results=n_results)

        # Prepare
        if query_records["embeddings"] is None:
            validate_record_set_for_embedding(record_set=query_records)
            request_embeddings = self._embed_record_set(
                record_set=query_records, is_query=True
            )
        else:
            request_embeddings = query_records["embeddings"]

        request_where = filters["where"]
        request_where_document = filters["where_document"]

        # We need to manually include uris in the result from the API to load datas
        request_include = include
        if "data" in request_include and "uris" not in request_include:
            request_include.append("uris")

        return QueryRequest(
            embeddings=request_embeddings,
            ids=filter_ids,
            where=request_where,
            where_document=request_where_document,
            include=request_include,
            n_results=n_results,
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
        update_records = normalize_insert_record_set(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        # Validate
        validate_insert_record_set(record_set=update_records)

        # Prepare
        if update_records["embeddings"] is None:
            # TODO: Handle URI updates.
            if (
                update_records["documents"] is not None
                or update_records["images"] is not None
            ):
                validate_record_set_for_embedding(
                    update_records, embeddable_fields={"documents", "images"}
                )
                update_embeddings = self._embed_record_set(record_set=update_records)
            else:
                update_embeddings = None
        else:
            update_embeddings = update_records["embeddings"]

        update_metadatas = self._apply_sparse_embeddings_to_metadatas(
            update_records["metadatas"], update_records["documents"]
        )

        return UpdateRequest(
            ids=update_records["ids"],
            embeddings=update_embeddings,
            metadatas=update_metadatas,
            documents=update_records["documents"],
            uris=update_records["uris"],
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
        upsert_records = normalize_insert_record_set(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        # Validate
        validate_insert_record_set(record_set=upsert_records)

        # Prepare
        if upsert_records["embeddings"] is None:
            validate_record_set_for_embedding(
                record_set=upsert_records, embeddable_fields={"documents", "images"}
            )
            upsert_embeddings = self._embed_record_set(record_set=upsert_records)
        else:
            upsert_embeddings = upsert_records["embeddings"]

        upsert_metadatas = self._apply_sparse_embeddings_to_metadatas(
            upsert_records["metadatas"], upsert_records["documents"]
        )

        return UpsertRequest(
            ids=upsert_records["ids"],
            metadatas=upsert_metadatas,
            embeddings=upsert_embeddings,
            documents=upsert_records["documents"],
            uris=upsert_records["uris"],
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
        else:
            request_ids = None
        filters = FilterSet(where=where, where_document=where_document)

        # Validate
        if request_ids is not None:
            validate_ids(ids=request_ids)
        validate_filter_set(filter_set=filters)

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
        self,
        name: Optional[str],
        metadata: Optional[CollectionMetadata],
        configuration: Optional[UpdateCollectionConfiguration],
    ) -> None:
        if name:
            self._model["name"] = name
        if metadata:
            self._model["metadata"] = metadata
        if configuration:
            self._model.set_configuration(
                overwrite_collection_configuration(
                    self._model.get_configuration(), configuration
                )
            )

            # If schema exists, also update it with the configuration changes
            if self.schema:
                from chromadb.api.collection_configuration import (
                    update_schema_from_collection_configuration,
                )

                updated_schema = update_schema_from_collection_configuration(
                    self.schema, configuration
                )
                self._model["serialized_schema"] = updated_schema.serialize_to_json()

    def _get_sparse_embedding_targets(self) -> Dict[str, "SparseVectorIndexConfig"]:
        schema = self.schema
        if schema is None:
            return {}

        targets: Dict[str, "SparseVectorIndexConfig"] = {}
        for key, value_types in schema.keys.items():
            if value_types.sparse_vector is None:
                continue
            sparse_index = value_types.sparse_vector.sparse_vector_index
            if sparse_index is None or not sparse_index.enabled:
                continue
            config = sparse_index.config
            if config.embedding_function is None or config.source_key is None:
                continue
            targets[key] = config

        return targets

    def _apply_sparse_embeddings_to_metadatas(
        self,
        metadatas: Optional[List[Metadata]],
        documents: Optional[List[Document]] = None,
    ) -> Optional[List[Metadata]]:
        sparse_targets = self._get_sparse_embedding_targets()
        if not sparse_targets:
            return metadatas

        # If no metadatas provided, create empty dicts based on documents length
        if metadatas is None:
            if documents is None:
                return None
            metadatas = [{} for _ in range(len(documents))]

        # Create copies, converting None to empty dict
        updated_metadatas: List[Dict[str, Any]] = [
            dict(metadata) if metadata is not None else {} for metadata in metadatas
        ]

        documents_list = list(documents) if documents is not None else None

        for target_key, config in sparse_targets.items():
            source_key = config.source_key
            embedding_func = config.embedding_function
            if source_key is None or embedding_func is None:
                continue

            if not isinstance(embedding_func, SparseEmbeddingFunction):
                embedding_func = cast(SparseEmbeddingFunction[Any], embedding_func)
            validate_sparse_embedding_function(embedding_func)

            # Initialize collection lists for batch processing
            inputs: List[str] = []
            positions: List[int] = []

            # Handle special case: source_key is "#document"
            if source_key == DOCUMENT_KEY:
                if documents_list is None:
                    continue

                # Collect documents that need embedding
                for idx, metadata in enumerate(updated_metadatas):
                    # Skip if target already exists in metadata
                    if target_key in metadata:
                        continue

                    # Get document at this position
                    if idx < len(documents_list):
                        doc = documents_list[idx]
                        if isinstance(doc, str):
                            inputs.append(doc)
                            positions.append(idx)

                # Generate embeddings for all collected documents
                if len(inputs) == 0:
                    continue

                sparse_embeddings = self._sparse_embed(
                    input=inputs,
                    sparse_embedding_function=embedding_func,
                )

                if len(sparse_embeddings) != len(positions):
                    raise ValueError(
                        "Sparse embedding function returned unexpected number of embeddings."
                    )

                for position, embedding in zip(positions, sparse_embeddings):
                    updated_metadatas[position][target_key] = embedding

                continue  # Skip the metadata-based logic below

            # Handle normal case: source_key is a metadata field
            for idx, metadata in enumerate(updated_metadatas):
                if target_key in metadata:
                    continue

                source_value = metadata.get(source_key)
                if not isinstance(source_value, str):
                    continue

                inputs.append(source_value)
                positions.append(idx)

            if len(inputs) == 0:
                continue

            sparse_embeddings = self._sparse_embed(
                input=inputs,
                sparse_embedding_function=embedding_func,
            )

            if len(sparse_embeddings) != len(positions):
                raise ValueError(
                    "Sparse embedding function returned unexpected number of embeddings."
                )

            for position, embedding in zip(positions, sparse_embeddings):
                updated_metadatas[position][target_key] = embedding

        # Convert empty dicts back to None, validation requires non-empty dicts or None
        result_metadatas: List[Optional[Metadata]] = [
            metadata if metadata else None for metadata in updated_metadatas
        ]

        validate_metadatas(cast(List[Metadata], result_metadatas))
        return cast(List[Metadata], result_metadatas)

    def _embed_record_set(
        self,
        record_set: BaseRecordSet,
        embeddable_fields: Optional[Set[str]] = None,
        is_query: bool = False,
    ) -> Embeddings:
        if embeddable_fields is None:
            embeddable_fields = get_default_embeddable_record_set_fields()

        for field in embeddable_fields:
            if record_set[field] is not None:  # type: ignore[literal-required]
                # uris require special handling
                if field == "uris":
                    if self._data_loader is None:
                        raise ValueError(
                            "You must set a data loader on the collection if loading from URIs."
                        )
                    return self._embed(
                        input=self._data_loader(uris=cast(URIs, record_set[field])),  # type: ignore[literal-required]
                        is_query=is_query,
                    )
                else:
                    return self._embed(
                        input=record_set[field],  # type: ignore[literal-required]
                        is_query=is_query,
                    )
        raise ValueError(
            "Record does not contain any non-None fields that can be embedded."
            f"Embeddable Fields: {embeddable_fields}"
            f"Record Fields: {record_set}"
        )

    def _embed(self, input: Any, is_query: bool = False) -> Embeddings:
        if self._embedding_function is not None and not isinstance(
            self._embedding_function, DefaultEmbeddingFunction
        ):
            if is_query:
                return self._embedding_function.embed_query(input=input)
            else:
                return self._embedding_function(input=input)

        config_ef = self.configuration.get("embedding_function")
        if config_ef is not None:
            if is_query:
                return config_ef.embed_query(input=input)
            else:
                return config_ef(input=input)
        schema = self.schema
        schema_embedding_function: Optional[EmbeddingFunction[Embeddable]] = None
        if schema is not None:
            override = schema.keys.get(EMBEDDING_KEY)
            if (
                override is not None
                and override.float_list is not None
                and override.float_list.vector_index is not None
                and override.float_list.vector_index.config.embedding_function
                is not None
            ):
                schema_embedding_function = cast(
                    EmbeddingFunction[Embeddable],
                    override.float_list.vector_index.config.embedding_function,
                )
            elif (
                schema.defaults.float_list is not None
                and schema.defaults.float_list.vector_index is not None
                and schema.defaults.float_list.vector_index.config.embedding_function
                is not None
            ):
                schema_embedding_function = cast(
                    EmbeddingFunction[Embeddable],
                    schema.defaults.float_list.vector_index.config.embedding_function,
                )

        if schema_embedding_function is not None:
            if is_query and hasattr(schema_embedding_function, "embed_query"):
                return schema_embedding_function.embed_query(input=input)
            return schema_embedding_function(input=input)
        if self._embedding_function is None:
            raise ValueError(
                "You must provide an embedding function to compute embeddings."
                "https://docs.trychroma.com/guides/embeddings"
            )
        if is_query:
            return self._embedding_function.embed_query(input=input)
        else:
            return self._embedding_function(input=input)

    def _sparse_embed(
        self,
        input: Any,
        sparse_embedding_function: SparseEmbeddingFunction[Any],
        is_query: bool = False,
    ) -> Any:
        if is_query:
            return sparse_embedding_function.embed_query(input=input)
        return sparse_embedding_function(input=input)

    def _embed_knn_string_queries(self, knn: Any) -> Any:
        """Embed string queries in Knn objects using the appropriate embedding function.

        Args:
            knn: A Knn object that may have a string query

        Returns:
            A Knn object with the string query replaced by an embedding

        Raises:
            ValueError: If the query is a string but no embedding function is available
        """
        from chromadb.execution.expression.operator import Knn

        if not isinstance(knn, Knn):
            return knn

        # If query is not a string, nothing to do
        if not isinstance(knn.query, str):
            return knn

        query_text = knn.query
        key = knn.key

        # Handle main embedding field
        if key == EMBEDDING_KEY:
            # Use the collection's main embedding function
            embedding = self._embed(input=[query_text], is_query=True)
            if not embedding or len(embedding) != 1:
                raise ValueError(
                    "Embedding function returned unexpected number of embeddings"
                )
            # Return a new Knn with the embedded query
            return Knn(
                query=embedding[0],
                key=knn.key,
                limit=knn.limit,
                default=knn.default,
                return_rank=knn.return_rank,
            )

        # Handle metadata field with potential sparse embedding
        schema = self.schema
        if schema is None or key not in schema.keys:
            raise ValueError(
                f"Cannot embed string query for key '{key}': "
                f"key not found in schema. Please provide an embedded vector or "
                f"configure an embedding function for this key in the schema."
            )

        value_type = schema.keys[key]

        # Check for sparse vector with embedding function
        if value_type.sparse_vector is not None:
            sparse_index = value_type.sparse_vector.sparse_vector_index
            if sparse_index is not None and sparse_index.enabled:
                sparse_config = sparse_index.config
                if sparse_config.embedding_function is not None:
                    embedding_func = sparse_config.embedding_function
                    if not isinstance(embedding_func, SparseEmbeddingFunction):
                        embedding_func = cast(
                            SparseEmbeddingFunction[Any], embedding_func
                        )
                    validate_sparse_embedding_function(embedding_func)

                    # Embed the query
                    sparse_embedding = self._sparse_embed(
                        input=[query_text],
                        sparse_embedding_function=embedding_func,
                        is_query=True,
                    )

                    if not sparse_embedding or len(sparse_embedding) != 1:
                        raise ValueError(
                            "Sparse embedding function returned unexpected number of embeddings"
                        )

                    # Return a new Knn with the sparse embedding
                    return Knn(
                        query=sparse_embedding[0],
                        key=knn.key,
                        limit=knn.limit,
                        default=knn.default,
                        return_rank=knn.return_rank,
                    )

        # Check for dense vector with embedding function (float_list)
        if value_type.float_list is not None:
            vector_index = value_type.float_list.vector_index
            if vector_index is not None and vector_index.enabled:
                dense_config = vector_index.config
                if dense_config.embedding_function is not None:
                    embedding_func = dense_config.embedding_function
                    validate_embedding_function(embedding_func)

                    # Embed the query using the schema's embedding function
                    try:
                        embeddings = embedding_func.embed_query(input=[query_text])
                    except AttributeError:
                        # Fallback if embed_query doesn't exist
                        embeddings = embedding_func([query_text])

                    if not embeddings or len(embeddings) != 1:
                        raise ValueError(
                            "Embedding function returned unexpected number of embeddings"
                        )

                    # Return a new Knn with the dense embedding
                    return Knn(
                        query=embeddings[0],
                        key=knn.key,
                        limit=knn.limit,
                        default=knn.default,
                        return_rank=knn.return_rank,
                    )

        raise ValueError(
            f"Cannot embed string query for key '{key}': "
            f"no embedding function configured for this key in the schema. "
            f"Please provide an embedded vector or configure an embedding function."
        )

    def _embed_rank_string_queries(self, rank: Any) -> Any:
        """Recursively embed string queries in Rank expressions.

        Args:
            rank: A Rank expression that may contain Knn objects with string queries

        Returns:
            A Rank expression with all string queries embedded
        """
        # Import here to avoid circular dependency
        from chromadb.execution.expression.operator import (
            Knn,
            Abs,
            Div,
            Exp,
            Log,
            Max,
            Min,
            Mul,
            Sub,
            Sum,
            Val,
            Rrf,
        )

        if rank is None:
            return None

        # Base case: Knn - embed if it has a string query
        if isinstance(rank, Knn):
            return self._embed_knn_string_queries(rank)

        # Base case: Val - no embedding needed
        if isinstance(rank, Val):
            return rank

        # Recursive cases: walk through child ranks
        if isinstance(rank, Abs):
            return Abs(self._embed_rank_string_queries(rank.rank))

        if isinstance(rank, Div):
            return Div(
                self._embed_rank_string_queries(rank.left),
                self._embed_rank_string_queries(rank.right),
            )

        if isinstance(rank, Exp):
            return Exp(self._embed_rank_string_queries(rank.rank))

        if isinstance(rank, Log):
            return Log(self._embed_rank_string_queries(rank.rank))

        if isinstance(rank, Max):
            return Max([self._embed_rank_string_queries(r) for r in rank.ranks])

        if isinstance(rank, Min):
            return Min([self._embed_rank_string_queries(r) for r in rank.ranks])

        if isinstance(rank, Mul):
            return Mul([self._embed_rank_string_queries(r) for r in rank.ranks])

        if isinstance(rank, Sub):
            return Sub(
                self._embed_rank_string_queries(rank.left),
                self._embed_rank_string_queries(rank.right),
            )

        if isinstance(rank, Sum):
            return Sum([self._embed_rank_string_queries(r) for r in rank.ranks])

        if isinstance(rank, Rrf):
            return Rrf(
                ranks=[self._embed_rank_string_queries(r) for r in rank.ranks],
                k=rank.k,
                weights=rank.weights,
                normalize=rank.normalize,
            )

        # Unknown rank type - return as is
        return rank

    def _embed_search_string_queries(self, search: Any) -> Any:
        """Embed string queries in a Search object.

        Args:
            search: A Search object that may contain Knn objects with string queries

        Returns:
            A Search object with all string queries embedded
        """
        # Import here to avoid circular dependency
        from chromadb.execution.expression.plan import Search

        if not isinstance(search, Search):
            return search

        # Embed the rank expression if it exists
        embedded_rank = self._embed_rank_string_queries(search._rank)

        # Create a new Search with the embedded rank
        return Search(
            where=search._where,
            rank=embedded_rank,
            group_by=search._group_by,
            limit=search._limit,
            select=search._select,
        )
