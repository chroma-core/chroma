from typing import TypedDict, Dict, Any, Optional, cast
import json
from chromadb.api.types import EmbeddingFunction, Embeddable, Space
from chromadb.utils.embedding_functions import (
    DefaultEmbeddingFunction,
    # known_embedding_functions,
)
from multiprocessing import cpu_count
import warnings


class HNSWConfiguration(TypedDict, total=False):
    ef_search: int
    num_threads: int
    batch_size: int
    sync_threshold: int
    resize_factor: float


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


class UpdateCollectionConfiguration(TypedDict, total=False):
    hnsw: Optional[HNSWConfiguration]
    embedding_function: Optional[EmbeddingFunction[Embeddable]]


def create_collection_config_to_json_str(config: CreateCollectionConfiguration) -> str:
    """Convert a CreateCollection configuration to a JSON-serializable string"""
    return json.dumps(create_collection_config_to_json(config))


def create_collection_config_to_json(
    config: CreateCollectionConfiguration,
) -> Dict[str, Any]:
    """Convert a CreateCollection configuration to a JSON-serializable dict"""
    if config.get("hnsw") is None:
        config["hnsw"] = create_default_create_hnsw_config()
    if config.get("embedding_function") is None:
        config["embedding_function"] = cast(
            EmbeddingFunction[Embeddable], DefaultEmbeddingFunction()
        )

    hnsw_config = cast(CreateHNSWConfiguration, config.get("hnsw"))

    try:
        ef = cast(EmbeddingFunction[Embeddable], config.get("embedding_function"))
        ef_config = {"name": ef.name(), "config": ef.get_config()}
    except Exception as e:
        warnings.warn(
            f"legacy embedding function config: {e}",
            DeprecationWarning,
            stacklevel=2,
        )
        ef = None
        ef_config = {"name": "legacy"}

    populate_hnsw_defaults(hnsw_config, ef)

    validate_create_hnsw_config(hnsw_config, ef)

    return {
        "hnsw": hnsw_config,
        "embedding_function": ef_config,
    }


def create_default_create_hnsw_config() -> CreateHNSWConfiguration:
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


def populate_hnsw_defaults(
    config: CreateHNSWConfiguration, ef: Optional[EmbeddingFunction[Embeddable]] = None
) -> None:
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


def validate_create_hnsw_config(
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
