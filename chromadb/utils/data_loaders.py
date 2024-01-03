import importlib
import multiprocessing
from typing import Optional, Sequence, List, Union
from operator import attrgetter
import numpy as np
from chromadb.api.types import URI, DataLoader, Image
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

    def _load_image(self, uri: Optional[URI]) -> Optional[Image]:
        return np.array(self._PILImage.open(uri)) if uri is not None else None

    def __call__(self, uris: Sequence[Optional[URI]]) -> List[Optional[Image]]:
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            return list(executor.map(self._load_image, uris))

# --------------------------- A lightweight util ---------------------------

# TODO: Placing this here, since data loaders are a common use case for this function.
#   But it should probably be moved to a more general-purpose module.
#   See: https://github.com/chroma-core/chroma/issues/1606
from functools import partial
from concurrent.futures import ThreadPoolExecutor

def vectorize(func, iterable=None, *, max_workers : int = 1):
    """Like builtin map, but returns a list, 
    and if iterable is None, returns a partial function that can directly be applied 
    to an iterable.
    This is useful, for instance, for making a data loader from any single-uri loader.
    
    Example:
    >>> vectorize(lambda x: x**2, [1,2,3])
    [1, 4, 9]
    >>> vectorized_square = vectorize(lambda x: x**2)
    >>> vectorized_square([1,2,3])
    [1, 4, 9]
    """
    if iterable is None:
        return partial(vectorize, func, max_workers=max_workers)
    if max_workers == 1:
        return list(map(func, iterable))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(func, iterable))
        
# --------------------------- Examples of loaders ---------------------------

FileContents = Union[str, bytes]  # a type for the contents of a file

def load_text(filepath: str) -> str:
    with open(filepath, "r") as f:
        return f.read()

def load_bytes(filepath: str) -> bytes:
    with open(filepath, "rb") as f:
        return f.read()
    
def url_to_contents(
        url: str, content_extractor=attrgetter('text'), *, params=None, **kwargs
    ) -> FileContents:
    import requests

    response = requests.get(url, params=params, **kwargs)
    response.raise_for_status()
    return content_extractor(response)

def pdf_file_text(
        filepath: str, *, page_break_delim='---------------------------'
    ) -> str:
    from pypdf import PdfReader  # pip install pypdf

    return page_break_delim.join(
        page.extract_text() for page in PdfReader(filepath).pages
    )

# --------------------------- FileLoader ---------------------------

class FileLoader(DataLoader[List[Optional[FileContents]]]):
    """A DataLoader that loads a list of text files from a list of URIs.

    By default, it loads text files from local files, given URIs that are full file paths.
    You can specify a prefix thought (usually used to specify a root directory),
    or a suffix (usually used to specify a file extension).

    Further, you can specify a different `loader`, e.g. to load files from a remote URL, 
    or to load binary files, or to load text from pdf files, or from S3, or a database, etc.

    Example:

    >>> rootdir = chromadb.__path__[0] + '/'
    >>> file_loader_1 = FileLoader(prefix=rootdir)
    >>> file_contents_1 = file_loader_1(['__init__.py', 'types.py'])
    >>> len(file_contents_1)
    2
    >>> 'Embeddings' in file_contents_1[0]  # i.e. __init__.py contains the word 'Embeddings'
    >>> 'from typing import' in file_contents_1[1]  # i.e. types.py contains the phrase 'from typing import'
    
    """
    
    def __init__(
        self,
        loader=load_text,
        *,
        prefix: str = "",
        suffix: str = "",
        max_workers: int = multiprocessing.cpu_count(),
    ) -> None:
        """
        Args:
            loader: A function that takes a (single) URI and returns its contents
            prefix: A string to prepend to the URI
            suffix: A string to append to the URI
            max_workers: The maximum number of threads to use when loading the URIs
        """
        self._loader = loader
        self._prefix = prefix
        self._suffix = suffix
        self._max_workers = max_workers

    def _load_file(self, uri: Optional[URI]) -> Optional[FileContents]:
        if uri is None:
            return None
        return self._loader(f"{self._prefix}{uri}{self._suffix}")

    def __call__(self, uris: Sequence[Optional[URI]]) -> List[Optional[FileContents]]:
        if isinstance(uris, str):
            # To avoid a common mistake, we cast a string to a list of containing it
            uris = [uris]
        return vectorize(self._load_file, uris, max_workers=self._max_workers)
        
# add a few loaders as attributes, for convenience
FileLoader.load_text = load_text
FileLoader.load_bytes = load_bytes
FileLoader.url_to_contents = url_to_contents
FileLoader.pdf_file_text = pdf_file_text