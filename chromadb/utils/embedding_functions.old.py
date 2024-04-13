import hashlib
import logging
from functools import cached_property

from tenacity import stop_after_attempt, wait_random, retry, retry_if_exception

from chromadb.api.types import (
    Document,
    Documents,
    Embedding,
    Image,
    Images,
    EmbeddingFunction,
    Embeddings,
    is_image,
    is_document,
)

from io import BytesIO
from pathlib import Path
import os
import tarfile
import requests
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Union, cast
import numpy as np
import numpy.typing as npt
import importlib
import inspect
import json
import sys
import base64

try:
    from chromadb.is_thin_client import is_thin_client
except ImportError:
    is_thin_client = False


logger = logging.getLogger(__name__)


def DefaultEmbeddingFunction() -> Optional[EmbeddingFunction[Documents]]:
    if is_thin_client:
        return None
    else:
        return ONNXMiniLM_L6_V2()

      
# List of all classes in this module
_classes = [
    name
    for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
    if obj.__module__ == __name__
]


def get_builtins() -> List[str]:
    return _classes
