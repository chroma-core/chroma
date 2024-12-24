import logging
from typing import Optional, Union, cast, List

import numpy as np

# -------------------------------------------------------------------------
# GeoClipEmbeddingFunction Module
# 
# Original GeoCLIP Reference:
# @inproceedings{geoclip,
#   title={GeoCLIP: Clip-Inspired Alignment between Locations and Images for Effective Worldwide Geo-localization},
#   author={Vivanco, Vicente and Nayak, Gaurav Kumar and Shah, Mubarak},
#   booktitle={Advances in Neural Information Processing Systems},
#   year={2023}
# }
#
# Custom Embedding Implementation by Andrew Herr (LatticeWorks)
# -------------------------------------------------------------------------
try:
    from geoclip import LocationEncoder
except ImportError:
    raise ImportError("The geoclip python package is not installed. Please install it with `pip install geoclip`.")

try:
    import torch
except ImportError:
    raise ImportError("The torch python package is not installed. Please install it with `pip install torch`")


from chromadb.api.types import (
    Document,
    Documents,
    Embedding,
    Embeddings,
    is_document,
)

# Initialize logger
logger = logging.getLogger(__name__)

class GeoClipEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    Implements an embedding function for geographic coordinates using the GeoCLIP model.
    Handles string "lat,lon", list [lat, lon], and dictionaries {"latitude": lat, "longitude": lon} as input.
    """

    def __init__(self, device: Optional[str] = None) -> None:
        """
        Initializes the GeoClipEmbeddingFunction.
        Loads the GeoCLIP location encoder model and configures the computation device.
        Args:
            device (Optional[str]): The device to use for computation ('cpu' or 'cuda').
                                     If not specified, auto-detection is performed.
        """
        self._gps_encoder = LocationEncoder()
        self._device = device or ("cuda" if torch.cuda.is_available() else "cpu")

        try:
            self._gps_encoder.to(self._device)
            torch.tensor([0.0], device=self._device)  # Test device availability
        except RuntimeError as e:
            logger.warning(f"Failed to move model to device {self._device}: {e}. Falling back to CPU.")
            self._device = "cpu"
            self._gps_encoder.to(self._device)

        logger.info(f"Using device: {self._device}")

    def _encode_coordinates(self, coordinates: Union[Document, List[float], dict]) -> Embedding:
        """Encodes a single coordinate pair."""
        try:
            # Handle different input formats
            if isinstance(coordinates, str):
                lat, lon = map(float, coordinates.strip().split(','))
            elif isinstance(coordinates, list) and len(coordinates) == 2:
                lat, lon = coordinates
            elif isinstance(coordinates, dict) and "latitude" in coordinates and "longitude" in coordinates:
                lat = coordinates["latitude"]
                lon = coordinates["longitude"]
            else:
                raise ValueError("Invalid coordinate format. Expected 'lat,lon' string, [lat, lon] list, or {'latitude': lat, 'longitude': lon} dict")

            # Validate latitude and longitude ranges
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                raise ValueError("Latitude and longitude out of range")

            # Convert coordinates to tensor
            gps_data = torch.tensor([[lat, lon]], dtype=torch.float32, device=self._device)
            with torch.no_grad():
                gps_embedding = self._gps_encoder(gps_data).squeeze().cpu().numpy()

            return cast(Embedding, gps_embedding)
        except ValueError as e:
            logger.warning(f"Could not parse coordinates: '{coordinates}'. Error: {e}")
            return cast(Embedding, np.zeros(512))

    def __call__(self, input: Documents) -> Embeddings:
        """Processes a list of documents and generates embeddings."""
        embeddings: Embeddings = []
        for item in input:
            # Check for valid formats
            if is_document(item) or (isinstance(item, list) and len(item) == 2) or (isinstance(item, dict) and "latitude" in item and "longitude" in item):
                embeddings.append(self._encode_coordinates(item))
            else:
                logger.warning(f"Skipping invalid input: {item}. Expected 'lat,lon' string, [lat, lon] list, or {'latitude': lat, 'longitude': lon} dict.")
                embeddings.append(cast(Embedding, np.zeros(512)))

        return embeddings
