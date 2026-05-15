import math
import multiprocessing
import re
from typing import Any, Callable, Dict, Union

from chromadb.types import Metadata


Validator = Callable[[Union[str, int, float]], bool]
MAX_RESIZE_FACTOR = 5.0


def _is_positive_int(value: Union[str, int, float]) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _is_resize_factor(value: Union[str, int, float]) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False

    try:
        resize_factor = float(value)
    except OverflowError:
        return False

    return math.isfinite(resize_factor) and 1.0 <= resize_factor <= MAX_RESIZE_FACTOR


param_validators: Dict[str, Validator] = {
    "hnsw:space": lambda p: bool(re.match(r"^(l2|cosine|ip)$", str(p))),
    "hnsw:construction_ef": _is_positive_int,
    "hnsw:search_ef": _is_positive_int,
    "hnsw:M": _is_positive_int,
    "hnsw:num_threads": _is_positive_int,
    "hnsw:resize_factor": _is_resize_factor,
}

# Extra params used for persistent hnsw
persistent_param_validators: Dict[str, Validator] = {
    "hnsw:batch_size": lambda p: isinstance(p, int) and p > 2,
    "hnsw:sync_threshold": lambda p: isinstance(p, int) and p > 2,
}


class Params:
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
    space: str
    construction_ef: int
    search_ef: int
    M: int
    num_threads: int
    resize_factor: float

    def __init__(self, metadata: Metadata):
        metadata = metadata or {}
        self.space = str(metadata.get("hnsw:space", "l2"))
        self.construction_ef = int(metadata.get("hnsw:construction_ef", 100))
        self.search_ef = int(metadata.get("hnsw:search_ef", 100))
        self.M = int(metadata.get("hnsw:M", 16))
        self.num_threads = int(
            metadata.get("hnsw:num_threads", multiprocessing.cpu_count())
        )
        self.resize_factor = float(metadata.get("hnsw:resize_factor", 1.2))

    @staticmethod
    def extract(metadata: Metadata) -> Metadata:
        """Validate and return only the relevant hnsw params"""
        segment_metadata = HnswParams._select(metadata)
        HnswParams._validate(segment_metadata, param_validators)
        return segment_metadata


class PersistentHnswParams(HnswParams):
    batch_size: int
    sync_threshold: int

    def __init__(self, metadata: Metadata):
        super().__init__(metadata)
        self.batch_size = int(metadata.get("hnsw:batch_size", 100))
        self.sync_threshold = int(metadata.get("hnsw:sync_threshold", 1000))

    @staticmethod
    def extract(metadata: Metadata) -> Metadata:
        """Returns only the relevant hnsw params"""
        all_validators = {**param_validators, **persistent_param_validators}
        segment_metadata = PersistentHnswParams._select(metadata)
        PersistentHnswParams._validate(segment_metadata, all_validators)
        return segment_metadata
