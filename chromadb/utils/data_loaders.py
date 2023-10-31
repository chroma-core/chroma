import importlib
from typing import Optional, Sequence, List
import numpy as np
from chromadb.api.types import URI, DataLoader, Image


class ImageLoader(DataLoader[List[Optional[Image]]]):
    def __init__(self) -> None:
        try:
            self._PILImage = importlib.import_module("PIL.Image")
        except ImportError:
            raise ValueError(
                "The PIL python package is not installed. Please install it with `pip install pillow`"
            )

    def __call__(self, uris: Sequence[Optional[URI]]) -> List[Optional[Image]]:
        return [
            np.array(self._PILImage.open(uri)) if uri is not None else None
            for uri in uris
        ]
