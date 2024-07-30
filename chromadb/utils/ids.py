import uuid
from typing import Generator, Optional, Any, Union

from chromadb.api.types import IDGenerator, ID, Embeddable, OneOrMany, Metadata
from chromadb.config import get_class


class UUIDGenerator(IDGenerator[Embeddable]):
    def __init__(self):
        ...

    def generator(self, documents: Optional[OneOrMany[Embeddable]] = None, metadatas: Optional[OneOrMany[Metadata]] = None) -> Generator[ID, None, None]:
        if documents:
            for _ in documents:
                yield f"{uuid.uuid4()}"
        elif metadatas:
            for _ in metadatas:
                yield f"{uuid.uuid4()}"
        else:
            while True:
                yield f"{uuid.uuid4()}"


def get_id_generator_instance(generator_type_or_inst: Union[str, Any]) -> IDGenerator:
    if isinstance(generator_type_or_inst, IDGenerator):
        return generator_type_or_inst
    elif isinstance(generator_type_or_inst, str):
        return get_class(generator_type_or_inst,IDGenerator[Embeddable])()
    else:
        raise ValueError(f"Unknown generator type: {generator_type_or_inst}")
