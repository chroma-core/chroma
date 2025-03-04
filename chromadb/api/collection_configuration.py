from typing import TypedDict, Dict, Any, Optional, cast
import json
from chromadb.api.types import EmbeddingFunction, Embeddable, Space, Metadata
from chromadb.utils.embedding_functions import (
    DefaultEmbeddingFunction,
    known_embedding_functions,
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


def json_to_hnsw_config(json_map: Dict[str, Any]) -> HNSWConfiguration:
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


def load_collection_config_from_json_str(json_str: str) -> CollectionConfiguration:
    json_map = json.loads(json_str)
    return load_collection_config_from_json(json_map)


def load_collection_config_from_json(
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
            return CollectionConfiguration(hnsw=json_to_hnsw_config(json_map["hnsw"]))
        else:
            if json_map["embedding_function"]["type"] == "legacy":
                warnings.warn(
                    "legacy embedding function config",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return CollectionConfiguration(
                    hnsw=json_to_hnsw_config(json_map["hnsw"])
                )
            else:
                ef = cast(
                    EmbeddingFunction[Embeddable],
                    known_embedding_functions[json_map["embedding_function"]["name"]],
                )
                return CollectionConfiguration(
                    hnsw=json_to_hnsw_config(json_map["hnsw"]),
                    embedding_function=ef.build_from_config(
                        json_map["embedding_function"]["config"]
                    ),
                )


class CreateHNSWConfiguration(TypedDict, total=False):
    space: Space
    ef_construction: int
    max_neighbors: int
    ef_search: int
    num_threads: int
    batch_size: int
    sync_threshold: int
    resize_factor: float


class CreateCollectionConfiguration(TypedDict, total=False):
    hnsw: Optional[CreateHNSWConfiguration]
    embedding_function: Optional[EmbeddingFunction[Embeddable]]


def load_collection_config_from_create_collection_config(
    config: CreateCollectionConfiguration,
) -> CollectionConfiguration:
    return CollectionConfiguration(
        hnsw=config.get("hnsw"), embedding_function=config.get("embedding_function")
    )


def create_collection_config_from_legacy_params(
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

    hnsw_config = json_to_hnsw_config(json_map)
    hnsw_config = populate_create_hnsw_defaults(hnsw_config)
    validate_hnsw_config(hnsw_config)

    return CreateCollectionConfiguration(hnsw=hnsw_config)


def create_collection_config_to_json_str(config: CreateCollectionConfiguration) -> str:
    """Convert a CreateCollection configuration to a JSON-serializable string"""
    return json.dumps(create_collection_config_to_json(config))


def create_collection_config_to_json(
    config: CreateCollectionConfiguration,
) -> Dict[str, Any]:
    """Convert a CreateCollection configuration to a JSON-serializable dict"""
    if config.get("hnsw") is None:
        config["hnsw"] = default_create_hnsw_config()
    if config.get("embedding_function") is None:
        config["embedding_function"] = cast(
            EmbeddingFunction[Embeddable], DefaultEmbeddingFunction()
        )
    try:
        hnsw_config = cast(CreateHNSWConfiguration, config.get("hnsw"))
    except Exception as e:
        raise ValueError(f"not a valid hnsw config: {e}")

    try:
        ef = cast(EmbeddingFunction[Embeddable], config.get("embedding_function"))
        ef_config = {
            "name": ef.name(),
            "type": "known" if ef.name() in known_embedding_functions else "custom",
            "config": ef.get_config(),
        }
    except Exception as e:
        warnings.warn(
            f"legacy embedding function config: {e}",
            DeprecationWarning,
            stacklevel=2,
        )
        ef = None
        ef_config = {"type": "legacy"}

    hnsw_config = populate_create_hnsw_defaults(hnsw_config, ef)

    validate_hnsw_config(hnsw_config, ef)

    return {
        "hnsw": hnsw_config,
        "embedding_function": ef_config,
    }


def default_create_hnsw_config() -> CreateHNSWConfiguration:
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


def validate_hnsw_config(
    config: CreateHNSWConfiguration, ef: Optional[EmbeddingFunction[Embeddable]] = None
) -> None:
    """Validate a CreateHNSW configuration"""
    if "batch_size" in config and "sync_threshold" in config:
        if config["batch_size"] > config["sync_threshold"]:
            raise ValueError("batch_size must be less than or equal to sync_threshold")
    if "ef_construction" in config and "max_neighbors" in config:
        if config["ef_construction"] > config["max_neighbors"]:
            raise ValueError(
                "ef_construction must be less than or equal to max_neighbors"
            )
    if "ef_search" in config and "max_neighbors" in config:
        if config["ef_search"] > config["max_neighbors"]:
            raise ValueError("ef_search must be less than or equal to max_neighbors")
    if "num_threads" in config:
        if config["num_threads"] > cpu_count():
            raise ValueError(
                "num_threads must be less than or equal to the number of available threads"
            )
    if "resize_factor" in config:
        if config["resize_factor"] <= 1:
            raise ValueError("resize_factor must be greater than 1")
    if "space" in config:
        if config["space"] not in Space.__members__.values():
            raise ValueError("space must be one of the following: cosine, l2, ip")
        if ef is not None and config["space"] not in ef.supported_spaces():
            raise ValueError("space must be supported by the embedding function")


# def json_to_create_collection_config(config: Dict[str, Any]) -> CreateCollectionConfiguration:
#     """Convert a JSON-serializable dict to a CreateCollectionConfiguration"""
#     if config.get("hnsw") is None:
#         raise ValueError("hnsw is required")
#     if config.get("embedding_function") is None:
#         raise ValueError("embedding_function is required")


#     return CreateCollectionConfiguration(
#         hnsw=json_to_create_hnsw_config(config["hnsw"]),
#         embedding_function=json_to_embedding_function(config["embedding_function"]),
#     )

# def json_to_create_hnsw_config(hnsw_config: Dict[str, Any]) -> CreateHNSWConfiguration:
#     """Convert a JSON-serializable dict to a CreateHNSWConfiguration"""
#     return CreateHNSWConfiguration(
#         **hnsw_config,
#     )

# def json_to_embedding_function(ef_config: Dict[str, Any]) -> Optional[EmbeddingFunction[Embeddable]]:
#     """Convert a JSON-serializable dict to an EmbeddingFunction"""
#     if ef_config["name"] == "legacy":
#         warnings.warn(
#             "legacy embedding function config",
#             DeprecationWarning,
#             stacklevel=2,
#         )
#         return None

#     if ef_config["name"] not in known_embedding_functions:
#         raise ValueError(f"unknown embedding function: {ef_config['name']}")

#     ef = known_embedding_functions[ef_config["name"]]

#     return cast(EmbeddingFunction[Embeddable], ef.build_from_config(ef_config["config"]))


class UpdateHNSWConfiguration(TypedDict, total=False):
    ef_search: int
    num_threads: int
    batch_size: int
    sync_threshold: int
    resize_factor: float


class UpdateCollectionConfiguration(TypedDict, total=False):
    hnsw: Optional[UpdateHNSWConfiguration]
    embedding_function: Optional[EmbeddingFunction[Embeddable]]
