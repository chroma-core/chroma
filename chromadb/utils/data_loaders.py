import importlib
import httpx
from io import BytesIO
import multiprocessing
from typing import Optional, Sequence, List, Tuple
import numpy as np
from chromadb.api.types import URI, DataLoader, Image, URIs
from concurrent.futures import ThreadPoolExecutor


class ImageLoader(DataLoader[List[Optional[Image]]]):
    def __init__(self, max_workers: int = multiprocessing.cpu_count()) -> None:
        try:
            self._PILImage = importlib.import_module("PIL.Image")
            self._max_workers = max_workers
        except ImportError:
            raise ValueError(
                "The PIL python package is not installed. Please install it with `pip install pillow`"
            )

    #    def _load_image(self, uri: Optional[URI]) -> Optional[Image]:
    #        return np.array(self._PILImage.open(uri)) if uri is not None else None

    def _load_image(self, uri: Optional[URI]) -> Optional[Image]:
        if uri is not None:
            if uri[0:4] == "http":
                return np.asarray(self._PILImage.open(BytesIO(httpx.get(uri).content)))
            else:
                return np.array(self._PILImage.open(uri))
        else:
            return None

    def __call__(self, uris: Sequence[Optional[URI]]) -> List[Optional[Image]]:
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            return list(executor.map(self._load_image, uris))


class ChromaLangchainPassthroughDataLoader(DataLoader[List[Optional[Image]]]):
    # This is a simple pass through data loader that just returns the input data with "images"
    # flag which lets the langchain embedding function know that the data is image uris
    def __call__(self, uris: URIs) -> Tuple[str, URIs]:  # type: ignore
        return ("images", uris)
