from typing import TypedDict, Dict, Any, Optional, cast
import json
from chromadb.api.types import (
    EmbeddingFunction,
    Embeddable,
    Space,
    Metadata,
    CollectionMetadata,
    UpdateMetadata,
)
from chromadb.utils.embedding_functions import (
    DefaultEmbeddingFunction,
    known_embedding_functions,
    register_embedding_function,
)
from multiprocessing import cpu_count
import warnings


class HNSWConfiguration(TypedDict, total=False):
    space: Space
    ef_construction: int
    max_neighbors: int
    ef_search: int
    num_threads: int
    batch_size: int
    sync_threshold: int
    resize_factor: float


def json_to_hnsw_configuration(json_map: Dict[str, Any]) -> HNSWConfiguration:
    config: HNSWConfiguration = {}
    if "space" in json_map:
        config["space"] = json_map["space"]
    if "ef_construction" in json_map:
        config["ef_construction"] = json_map["ef_construction"]
    if "max_neighbors" in json_map:
        config["max_neighbors"] = json_map["max_neighbors"]
    if "ef_search" in json_map:
        config["ef_search"] = json_map["ef_search"]
    if "num_threads" in json_map:
        config["num_threads"] = json_map["num_threads"]
    if "batch_size" in json_map:
        config["batch_size"] = json_map["batch_size"]
    if "sync_threshold" in json_map:
        config["sync_threshold"] = json_map["sync_threshold"]
    if "resize_factor" in json_map:
        config["resize_factor"] = json_map["resize_factor"]
    return config


class CollectionConfiguration(TypedDict, total=False):
    hnsw: Optional[HNSWConfiguration]
    embedding_function: Optional[EmbeddingFunction[Embeddable]]


def load_collection_configuration_from_json_str(
    json_str: str,
) -> CollectionConfiguration:
    json_map = json.loads(json_str)
    return load_collection_configuration_from_json(json_map)


def load_collection_configuration_from_json(
    json_map: Dict[str, Any]
) -> CollectionConfiguration:
    if "hnsw" not in json_map:
        if "embedding_function" not in json_map:
            return CollectionConfiguration()
        else:
            if json_map["embedding_function"]["type"] == "legacy":
                warnings.warn(
                    "legacy embedding function config",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return CollectionConfiguration()
            else:
                ef = cast(
                    EmbeddingFunction[Embeddable],
                    known_embedding_functions[json_map["embedding_function"]["name"]],
                )
                return CollectionConfiguration(
                    embedding_function=ef.build_from_config(
                        json_map["embedding_function"]["config"]
                    )
                )
    else:
        if "embedding_function" not in json_map:
            return CollectionConfiguration(
                hnsw=json_to_hnsw_configuration(json_map["hnsw"])
            )
        else:
            if json_map["embedding_function"]["type"] == "legacy":
                warnings.warn(
                    "legacy embedding function config",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return CollectionConfiguration(
                    hnsw=json_to_hnsw_configuration(json_map["hnsw"])
                )
            else:
                ef = cast(
                    EmbeddingFunction[Embeddable],
                    known_embedding_functions[json_map["embedding_function"]["name"]],
                )
                return CollectionConfiguration(
                    hnsw=json_to_hnsw_configuration(json_map["hnsw"]),
                    embedding_function=ef.build_from_config(
                        json_map["embedding_function"]["config"]
                    ),
                )


def collection_configuration_to_json_str(config: CollectionConfiguration) -> str:
    return json.dumps(collection_configuration_to_json(config))


def collection_configuration_to_json(config: CollectionConfiguration) -> Dict[str, Any]:
    hnsw_config = config.get("hnsw")

    # Handle embedding function serialization
    ef_config: Dict[str, Any] | None = None
    ef = config.get("embedding_function")
    if ef is not None:
        try:
            if ef.name() is NotImplemented:
                ef_config = {"type": "legacy"}
            else:
                ef_config = {
                    "name": ef.name(),
                    "type": "known"
                    if ef.name() in known_embedding_functions
                    else "custom",
                    "config": ef.get_config(),
                }
                register_embedding_function(type(ef))
        except Exception as e:
            warnings.warn(
                f"legacy embedding function config: {e}",
                DeprecationWarning,
                stacklevel=2,
            )
            ef_config = {"type": "legacy"}
    else:
        ef_config = None

    return {
        "hnsw": hnsw_config,
        "embedding_function": ef_config,
    }


class CreateHNSWConfiguration(TypedDict, total=False):
    space: Space
    ef_construction: int
    max_neighbors: int
    ef_search: int
    num_threads: int
    batch_size: int
    sync_threshold: int
    resize_factor: float


def json_to_create_hnsw_configuration(
    json_map: Dict[str, Any]
) -> CreateHNSWConfiguration:
    config: CreateHNSWConfiguration = {}
    if "space" in json_map:
        config["space"] = json_map["space"]
    if "ef_construction" in json_map:
        config["ef_construction"] = json_map["ef_construction"]
    if "max_neighbors" in json_map:
        config["max_neighbors"] = json_map["max_neighbors"]
    if "ef_search" in json_map:
        config["ef_search"] = json_map["ef_search"]
    if "num_threads" in json_map:
        config["num_threads"] = json_map["num_threads"]
    if "batch_size" in json_map:
        config["batch_size"] = json_map["batch_size"]
    if "sync_threshold" in json_map:
        config["sync_threshold"] = json_map["sync_threshold"]
    if "resize_factor" in json_map:
        config["resize_factor"] = json_map["resize_factor"]
    return config


class CreateCollectionConfiguration(TypedDict, total=False):
    hnsw: Optional[CreateHNSWConfiguration]
    embedding_function: Optional[EmbeddingFunction[Embeddable]]


def load_collection_configuration_from_create_collection_configuration(
    config: CreateCollectionConfiguration,
) -> CollectionConfiguration:
    return CollectionConfiguration(
        hnsw=config.get("hnsw"), embedding_function=config.get("embedding_function")
    )


def create_collection_configuration_from_legacy_collection_metadata(
    metadata: CollectionMetadata,
) -> CreateCollectionConfiguration:
    """Create a CreateCollectionConfiguration from legacy collection metadata"""
    old_to_new = {
        "hnsw:space": "space",
        "hnsw:ef_construction": "ef_construction",
        "hnsw:max_neighbors": "max_neighbors",
        "hnsw:ef_search": "ef_search",
        "hnsw:num_threads": "num_threads",
        "hnsw:batch_size": "batch_size",
        "hnsw:sync_threshold": "sync_threshold",
        "hnsw:resize_factor": "resize_factor",
    }
    json_map = {}
    for name, value in metadata.items():
        if name not in old_to_new:
            raise ValueError(f"unknown legacy parameter: {name}")
        json_map[old_to_new[name]] = value
    hnsw_config = json_to_create_hnsw_configuration(json_map)
    hnsw_config = populate_create_hnsw_defaults(hnsw_config)
    validate_create_hnsw_config(hnsw_config)

    return CreateCollectionConfiguration(hnsw=hnsw_config)


def create_collection_configuration_from_legacy_metadata(
    metadata: Metadata,
) -> CreateCollectionConfiguration:
    """Create a CreateCollectionConfiguration from legacy parameters"""
    old_to_new = {
        "hnsw:space": "space",
        "hnsw:ef_construction": "ef_construction",
        "hnsw:max_neighbors": "max_neighbors",
        "hnsw:ef_search": "ef_search",
        "hnsw:num_threads": "num_threads",
        "hnsw:batch_size": "batch_size",
        "hnsw:sync_threshold": "sync_threshold",
        "hnsw:resize_factor": "resize_factor",
    }
    json_map = {}
    for name, value in metadata.items():
        if name not in old_to_new:
            raise ValueError(f"unknown legacy parameter: {name}")
        json_map[old_to_new[name]] = value

    hnsw_config = json_to_create_hnsw_configuration(json_map)
    hnsw_config = populate_create_hnsw_defaults(hnsw_config)
    validate_create_hnsw_config(hnsw_config)

    return CreateCollectionConfiguration(hnsw=hnsw_config)


def legacy_create_collection_configuration_path(
    embedding_function: Optional[EmbeddingFunction[Embeddable]] = None,
    metadata: Optional[CollectionMetadata] = None,
) -> CreateCollectionConfiguration:
    configuration = CreateCollectionConfiguration()
    if embedding_function is None:
        configuration["embedding_function"] = DefaultEmbeddingFunction()  # type: ignore
    else:
        configuration["embedding_function"] = embedding_function
    if metadata is not None:
        configuration = create_collection_configuration_from_legacy_collection_metadata(
            metadata
        )
    return configuration


def load_create_collection_configuration_from_json_str(
    json_str: str,
) -> CreateCollectionConfiguration:
    json_map = json.loads(json_str)
    return load_create_collection_configuration_from_json(json_map)


def load_create_collection_configuration_from_json(
    json_map: Dict[str, Any]
) -> CreateCollectionConfiguration:
    if "hnsw" not in json_map:
        if "embedding_function" not in json_map:
            return CreateCollectionConfiguration()
        else:
            if json_map["embedding_function"]["type"] == "legacy":
                warnings.warn(
                    "legacy embedding function config",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return CreateCollectionConfiguration()
            else:
                ef = cast(
                    EmbeddingFunction[Embeddable],
                    known_embedding_functions[json_map["embedding_function"]["name"]],
                )
                return CollectionConfiguration(
                    embedding_function=ef.build_from_config(
                        json_map["embedding_function"]["config"]
                    )
                )
    else:
        if "embedding_function" not in json_map:
            return CreateCollectionConfiguration(
                hnsw=json_to_create_hnsw_configuration(json_map["hnsw"])
            )
        else:
            if json_map["embedding_function"]["type"] == "legacy":
                warnings.warn(
                    "legacy embedding function config",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return CreateCollectionConfiguration(
                    hnsw=json_to_create_hnsw_configuration(json_map["hnsw"])
                )
            else:
                ef = cast(
                    EmbeddingFunction[Embeddable],
                    known_embedding_functions[json_map["embedding_function"]["name"]],
                )
                return CreateCollectionConfiguration(
                    hnsw=json_to_create_hnsw_configuration(json_map["hnsw"]),
                    embedding_function=ef.build_from_config(
                        json_map["embedding_function"]["config"]
                    ),
                )


def create_collection_configuration_to_json_str(
    config: CreateCollectionConfiguration,
) -> str:
    """Convert a CreateCollection configuration to a JSON-serializable string"""
    return json.dumps(create_collection_configuration_to_json(config))


def create_collection_configuration_to_json(
    config: CreateCollectionConfiguration,
) -> Dict[str, Any]:
    """Convert a CreateCollection configuration to a JSON-serializable dict"""
    if config.get("hnsw") is None:
        config["hnsw"] = default_create_hnsw_configuration()
    if config.get("embedding_function") is None:
        config["embedding_function"] = cast(
            EmbeddingFunction[Embeddable], DefaultEmbeddingFunction()
        )
    try:
        hnsw_config = cast(CreateHNSWConfiguration, config.get("hnsw"))
    except Exception as e:
        raise ValueError(f"not a valid hnsw config: {e}")

    ef_config: Dict[str, Any] | None = None
    try:
        ef = cast(EmbeddingFunction[Embeddable], config.get("embedding_function"))
        if ef.name() is NotImplemented:
            ef_config = {"type": "legacy"}
        else:
            ef_config = {
                "name": ef.name(),
                "type": "known" if ef.name() in known_embedding_functions else "custom",
                "config": ef.get_config(),
            }
            register_embedding_function(type(ef))
    except Exception as e:
        warnings.warn(
            f"legacy embedding function config: {e}",
            DeprecationWarning,
            stacklevel=2,
        )
        ef = None
        ef_config = {"type": "legacy"}

    hnsw_config = populate_create_hnsw_defaults(hnsw_config, ef)

    validate_create_hnsw_config(hnsw_config, ef)

    return {
        "hnsw": hnsw_config,
        "embedding_function": ef_config,
    }


def default_create_hnsw_configuration() -> CreateHNSWConfiguration:
    """Create a default CreateHNSW configuration"""
    return CreateHNSWConfiguration(
        space=cast(Space, "cosine"),
        ef_construction=100,
        max_neighbors=16,
        ef_search=10,
        num_threads=cpu_count(),
        batch_size=100,
        sync_threshold=1000,
        resize_factor=1.2,
    )


def populate_create_hnsw_defaults(
    config: CreateHNSWConfiguration, ef: Optional[EmbeddingFunction[Embeddable]] = None
) -> CreateHNSWConfiguration:
    """Populate a CreateHNSW configuration with default values"""
    if config.get("space") is None:
        config["space"] = ef.default_space() if ef else cast(Space, "cosine")
    if config.get("ef_construction") is None:
        config["ef_construction"] = 100
    if config.get("max_neighbors") is None:
        config["max_neighbors"] = 16
    if config.get("ef_search") is None:
        config["ef_search"] = 10
    if config.get("num_threads") is None:
        config["num_threads"] = cpu_count()
    if config.get("batch_size") is None:
        config["batch_size"] = 100
    if config.get("sync_threshold") is None:
        config["sync_threshold"] = 1000
    if config.get("resize_factor") is None:
        config["resize_factor"] = 1.2
    return config


def validate_create_hnsw_config(
    config: CreateHNSWConfiguration, ef: Optional[EmbeddingFunction[Embeddable]] = None
) -> None:
    """Validate a CreateHNSW configuration"""
    if "batch_size" in config and "sync_threshold" in config:
        if config["batch_size"] > config["sync_threshold"]:
            raise ValueError("batch_size must be less than or equal to sync_threshold")
    if "num_threads" in config:
        if config["num_threads"] > cpu_count():
            raise ValueError(
                "num_threads must be less than or equal to the number of available threads"
            )
        if config["num_threads"] <= 0:
            raise ValueError("num_threads must be greater than 0")
    if "resize_factor" in config:
        if config["resize_factor"] <= 0:
            raise ValueError("resize_factor must be greater than 0")
    if "space" in config:
        # Check if the space value is one of the string values of the Space enum
        valid_spaces = [space.value for space in Space]
        space_value = config["space"]
        space_str = space_value.value if isinstance(space_value, Space) else space_value

        if space_str not in valid_spaces:
            raise ValueError(
                f"space must be one of the following: {', '.join(valid_spaces)}"
            )
        if ef is not None:
            supported_spaces = [space.value for space in ef.supported_spaces()]
            if space_str not in supported_spaces:
                raise ValueError("space must be supported by the embedding function")
    if "ef_construction" in config:
        if config["ef_construction"] <= 0:
            raise ValueError("ef_construction must be greater than 0")
    if "max_neighbors" in config:
        if config["max_neighbors"] <= 0:
            raise ValueError("max_neighbors must be greater than 0")
    if "ef_search" in config:
        if config["ef_search"] <= 0:
            raise ValueError("ef_search must be greater than 0")


class UpdateHNSWConfiguration(TypedDict, total=False):
    ef_search: int
    num_threads: int
    batch_size: int
    sync_threshold: int
    resize_factor: float


def json_to_update_hnsw_configuration(
    json_map: Dict[str, Any]
) -> UpdateHNSWConfiguration:
    config: UpdateHNSWConfiguration = {}
    if "ef_search" in json_map:
        config["ef_search"] = json_map["ef_search"]
    if "num_threads" in json_map:
        config["num_threads"] = json_map["num_threads"]
    if "batch_size" in json_map:
        config["batch_size"] = json_map["batch_size"]
    if "sync_threshold" in json_map:
        config["sync_threshold"] = json_map["sync_threshold"]
    if "resize_factor" in json_map:
        config["resize_factor"] = json_map["resize_factor"]
    return config


def validate_update_hnsw_config(
    config: UpdateHNSWConfiguration,
) -> None:
    """Validate an UpdateHNSW configuration"""
    if "ef_search" in config:
        if config["ef_search"] <= 0:
            raise ValueError("ef_search must be greater than 0")
    if "num_threads" in config:
        if config["num_threads"] > cpu_count():
            raise ValueError(
                "num_threads must be less than or equal to the number of available threads"
            )
        if config["num_threads"] <= 0:
            raise ValueError("num_threads must be greater than 0")
    if "batch_size" in config and "sync_threshold" in config:
        if config["batch_size"] > config["sync_threshold"]:
            raise ValueError("batch_size must be less than or equal to sync_threshold")
    if "resize_factor" in config:
        if config["resize_factor"] <= 0:
            raise ValueError("resize_factor must be greater than 0")


class UpdateCollectionConfiguration(TypedDict, total=False):
    hnsw: Optional[UpdateHNSWConfiguration]
    embedding_function: Optional[EmbeddingFunction[Embeddable]]


def update_collection_configuration_from_legacy_collection_metadata(
    metadata: CollectionMetadata,
) -> UpdateCollectionConfiguration:
    """Create an UpdateCollectionConfiguration from legacy collection metadata"""
    old_to_new = {
        "hnsw:ef_search": "ef_search",
        "hnsw:num_threads": "num_threads",
        "hnsw:batch_size": "batch_size",
        "hnsw:sync_threshold": "sync_threshold",
        "hnsw:resize_factor": "resize_factor",
    }
    json_map = {}
    for name, value in metadata.items():
        if name not in old_to_new:
            raise ValueError(f"unknown legacy parameter: {name}")
        json_map[old_to_new[name]] = value
    hnsw_config = json_to_update_hnsw_configuration(json_map)
    validate_update_hnsw_config(hnsw_config)
    return UpdateCollectionConfiguration(hnsw=hnsw_config)


def update_collection_configuration_from_legacy_update_metadata(
    metadata: UpdateMetadata,
) -> UpdateCollectionConfiguration:
    """Create an UpdateCollectionConfiguration from legacy update metadata"""
    old_to_new = {
        "hnsw:ef_search": "ef_search",
        "hnsw:num_threads": "num_threads",
        "hnsw:batch_size": "batch_size",
        "hnsw:sync_threshold": "sync_threshold",
        "hnsw:resize_factor": "resize_factor",
    }
    json_map = {}
    for name, value in metadata.items():
        if name not in old_to_new:
            raise ValueError(f"unknown legacy parameter: {name}")
        json_map[old_to_new[name]] = value
    hnsw_config = json_to_update_hnsw_configuration(json_map)
    validate_update_hnsw_config(hnsw_config)
    return UpdateCollectionConfiguration(hnsw=hnsw_config)


def update_collection_configuration_to_json_str(
    config: UpdateCollectionConfiguration,
) -> str:
    """Convert an UpdateCollectionConfiguration to a JSON-serializable string"""
    json_dict = update_collection_configuration_to_json(config)
    return json.dumps(json_dict)


def update_collection_configuration_to_json(
    config: UpdateCollectionConfiguration,
) -> Dict[str, Any]:
    """Convert an UpdateCollectionConfiguration to a JSON-serializable dict"""
    if config.get("hnsw") is None:
        return {}

    try:
        hnsw_config = cast(UpdateHNSWConfiguration, config.get("hnsw"))
    except Exception as e:
        raise ValueError(f"not a valid hnsw config: {e}")

    validate_update_hnsw_config(hnsw_config)
    ef_config: Dict[str, Any] | None = None
    ef = config.get("embedding_function")
    if ef is not None:
        if ef.name() is NotImplemented:
            ef_config = {"type": "legacy"}
        else:
            ef_config = {
                "name": ef.name(),
                "type": "known" if ef.name() in known_embedding_functions else "custom",
                "config": ef.get_config(),
            }
            register_embedding_function(type(ef))
    else:
        ef_config = None

    return {
        "hnsw": hnsw_config,
        "embedding_function": ef_config,
    }


def load_update_collection_configuration_from_json_str(
    json_str: str,
) -> UpdateCollectionConfiguration:
    json_map = json.loads(json_str)
    return load_update_collection_configuration_from_json(json_map)


def load_update_collection_configuration_from_json(
    json_map: Dict[str, Any]
) -> UpdateCollectionConfiguration:
    if "hnsw" not in json_map:
        if "embedding_function" not in json_map:
            return UpdateCollectionConfiguration()
        else:
            if json_map["embedding_function"]["type"] == "legacy":
                warnings.warn(
                    "legacy embedding function config",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return UpdateCollectionConfiguration()
            else:
                ef = cast(
                    EmbeddingFunction[Embeddable],
                    known_embedding_functions[json_map["embedding_function"]["name"]],
                )
                return UpdateCollectionConfiguration(
                    embedding_function=ef.build_from_config(
                        json_map["embedding_function"]["config"]
                    )
                )
    else:
        if "embedding_function" not in json_map:
            return UpdateCollectionConfiguration(
                hnsw=json_to_update_hnsw_configuration(json_map["hnsw"])
            )
        else:
            if json_map["embedding_function"]["type"] == "legacy":
                warnings.warn(
                    "legacy embedding function config",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return UpdateCollectionConfiguration(
                    hnsw=json_to_update_hnsw_configuration(json_map["hnsw"])
                )
            else:
                ef = cast(
                    EmbeddingFunction[Embeddable],
                    known_embedding_functions[json_map["embedding_function"]["name"]],
                )
                return UpdateCollectionConfiguration(
                    hnsw=json_to_update_hnsw_configuration(json_map["hnsw"]),
                    embedding_function=ef.build_from_config(
                        json_map["embedding_function"]["config"]
                    ),
                )


def overwrite_hnsw_configuration(
    existing_hnsw_config: HNSWConfiguration, update_hnsw_config: UpdateHNSWConfiguration
) -> HNSWConfiguration:
    """Overwrite a HNSWConfiguration with a new configuration"""
    if update_hnsw_config.get("ef_search") is not None:
        existing_hnsw_config["ef_search"] = update_hnsw_config["ef_search"]
    if update_hnsw_config.get("num_threads") is not None:
        existing_hnsw_config["num_threads"] = update_hnsw_config["num_threads"]
    if update_hnsw_config.get("batch_size") is not None:
        existing_hnsw_config["batch_size"] = update_hnsw_config["batch_size"]
    if update_hnsw_config.get("sync_threshold") is not None:
        existing_hnsw_config["sync_threshold"] = update_hnsw_config["sync_threshold"]
    if update_hnsw_config.get("resize_factor") is not None:
        existing_hnsw_config["resize_factor"] = update_hnsw_config["resize_factor"]
    return existing_hnsw_config


def overwrite_embedding_function(
    existing_embedding_function: EmbeddingFunction[Embeddable],
    update_embedding_function: EmbeddingFunction[Embeddable],
) -> EmbeddingFunction[Embeddable]:
    """Overwrite an EmbeddingFunction with a new configuration"""
    if update_embedding_function.name() is NotImplemented:
        warnings.warn(
            "cannot update legacy embedding function config",
            DeprecationWarning,
            stacklevel=2,
        )
        return existing_embedding_function
    if existing_embedding_function.name() is NotImplemented:
        warnings.warn(
            "cannot update legacy embedding function config",
            DeprecationWarning,
            stacklevel=2,
        )
        return existing_embedding_function

    if existing_embedding_function.name() != update_embedding_function.name():
        raise ValueError("cannot update embedding function with different name")

    update_embedding_function.validate_config_update(
        existing_embedding_function.get_config(), update_embedding_function.get_config()
    )
    return update_embedding_function


def overwrite_collection_configuration(
    existing_config: CollectionConfiguration,
    update_config: UpdateCollectionConfiguration,
) -> CollectionConfiguration:
    """Overwrite a CollectionConfiguration with a new configuration"""
    existing_hnsw_config = existing_config.get("hnsw")
    new_hnsw_config = update_config.get("hnsw")
    existing_embedding_function = existing_config.get("embedding_function")
    new_embedding_function = update_config.get("embedding_function")
    if existing_hnsw_config is not None:
        if new_hnsw_config is not None:
            updated_hnsw_config = overwrite_hnsw_configuration(
                existing_hnsw_config, new_hnsw_config
            )
    if existing_embedding_function is not None:
        if new_embedding_function is not None:
            updated_embedding_function = overwrite_embedding_function(
                existing_embedding_function, new_embedding_function
            )
    return CollectionConfiguration(
        hnsw=updated_hnsw_config, embedding_function=updated_embedding_function
    )


class InvalidConfigurationError(ValueError):
    """Represents an error that occurs when a configuration is invalid."""

    pass
