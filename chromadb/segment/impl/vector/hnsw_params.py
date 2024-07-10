import re
from typing import Any, Callable, Dict, Union

from chromadb.types import Metadata

# 07.2024: This file is entirely deprecated for external use. It is only used internally to
# extract legacy HNSW parameters from metadata. The new configuration system should be used
# instead. This file will be removed in the future.
# See chromadb.api.configuration for the new configuration system.


Validator = Callable[[Union[str, int, float]], bool]

param_validators: Dict[str, Validator] = {
    "hnsw:space": lambda p: bool(re.match(r"^(l2|cosine|ip)$", str(p))),
    "hnsw:construction_ef": lambda p: isinstance(p, int),
    "hnsw:search_ef": lambda p: isinstance(p, int),
    "hnsw:M": lambda p: isinstance(p, int),
    "hnsw:num_threads": lambda p: isinstance(p, int),
    "hnsw:resize_factor": lambda p: isinstance(p, (int, float)),
}

# Extra params used for persistent hnsw
persistent_param_validators: Dict[str, Validator] = {
    "hnsw:batch_size": lambda p: isinstance(p, int) and p > 2,
    "hnsw:sync_threshold": lambda p: isinstance(p, int) and p > 2,
}


class Params:
    """Deprecated. Use chromadb.api.configuration instead."""

    @staticmethod
    def _select(metadata: Metadata) -> Dict[str, Any]:
        segment_metadata = {}
        for param, value in metadata.items():
            if param.startswith("hnsw:"):
                segment_metadata[param] = value
        return segment_metadata

    @staticmethod
    def _validate(metadata: Dict[str, Any], validators: Dict[str, Validator]) -> None:
        """Validates the metadata"""
        # Validate it
        for param, value in metadata.items():
            if param not in validators:
                raise ValueError(f"Unknown HNSW parameter: {param}")
            if not validators[param](value):
                raise ValueError(f"Invalid value for HNSW parameter: {param} = {value}")


class HnswParams(Params):
    """Deprecated. Use chromadb.api.configuration instead."""

    def __init__(self, metadata: Metadata):
        raise NotImplementedError(
            "PersistentHnswParams is deprecated. Use chromadb.api.configuration instead."
        )

    @staticmethod
    def extract(metadata: Metadata) -> Metadata:
        """Validate and return only the relevant hnsw params
        Only used to extract legact HNSW params from metadata
        """
        segment_metadata = HnswParams._select(metadata)
        HnswParams._validate(segment_metadata, param_validators)
        return segment_metadata


class PersistentHnswParams(HnswParams):
    def __init__(self, metadata: Metadata):
        raise NotImplementedError(
            "PersistentHnswParams is deprecated. Use chromadb.api.configuration instead."
        )

    @staticmethod
    def extract(metadata: Metadata) -> Metadata:
        """Returns only the relevant hnsw params
        Only used to extract legact HNSW params from metadata
        """
        all_validators = {**param_validators, **persistent_param_validators}
        segment_metadata = PersistentHnswParams._select(metadata)
        PersistentHnswParams._validate(segment_metadata, all_validators)
        return segment_metadata
