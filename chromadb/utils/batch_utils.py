from typing import Any, Iterator, List, Optional, Tuple, TypeVar, Union, cast
from chromadb.api import ClientAPI
import numpy as np
from rich.progress import track

T = TypeVar("T", bound=Tuple[Union[List[Any], np.ndarray[Any, Any], None], ...])


def create_batches(
    client: ClientAPI,
    records: T,
    max_batch_size: Optional[int] = 1024,
    print_progress_description: Optional[str] = None,
) -> Iterator[T]:
    """
    Takes tuples like `([0, 1], [2, 3])` and yields batches of the tuple like `([0], [2])` and `([1], [3])`.

    For example:

    ```python
    import chromadb
    from chromadb.utils.batch_utils import create_batches
    import numpy as np

    client = chromadb.Client()
    collection = client.create_collection("foo")

    ids = [str(i) for i in range(100_000)]
    embeddings = np.random.rand(100_000, 128)

    for (ids, embeddings) in create_batches(client, (ids, embeddings), print_progress_description="Adding documents..."):
        collection.add(ids=ids, embeddings=embeddings)
    ```

    Args:
        client: A chromadb client
        records: A tuple of lists or numpy arrays
        max_batch_size: The maximum batch size to use, defaults to 1024
        print_progress_description: If specified, a progress bar will be displayed with this description
    """
    max_batch_size = min(client.get_max_batch_size(), max_batch_size or 1024)

    set_size = -1
    for field in records:
        if isinstance(field, list):
            set_size = len(field)
            break

    if set_size == -1:
        raise ValueError("Records must contain a list field")

    for i in track(
        range(0, set_size, max_batch_size),
        description=print_progress_description or "",
        disable=not print_progress_description,
    ):
        yield cast(
            T,
            tuple(
                None if field is None else field[i : i + max_batch_size]
                for field in records
            ),
        )
