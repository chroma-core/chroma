import os
import importlib
import pkgutil
from types import ModuleType
from typing import Optional, Set, cast

from chromadb.api.types import Documents, EmbeddingFunction

# Langchain embedding function is a special snowflake
from chromadb.utils.embedding_functions.chroma_langchain_embedding_function import (  # noqa: F401
    create_langchain_embedding,
)

_all_classes: Set[str] = set()
_all_classes.add("ChromaLangchainEmbeddingFunction")

try:
    from chromadb.is_thin_client import is_thin_client
except ImportError:
    is_thin_client = False


def _import_all_efs() -> Set[str]:
    imported_classes = set()
    _module_dir = os.path.dirname(__file__)
    for _, module_name, _ in pkgutil.iter_modules([_module_dir]):
        # Skip the current module
        if module_name == __name__:
            continue

        module: ModuleType = importlib.import_module(f"{__name__}.{module_name}")

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and isinstance(attr, EmbeddingFunction)
                and attr  # type: ignore[comparison-overlap]
                is not EmbeddingFunction  # Don't re-export the type
            ):
                globals()[attr.__name__] = attr
                imported_classes.add(attr.__name__)
    return imported_classes


_all_classes.update(_import_all_efs())


# Define and export the default embedding function
def DefaultEmbeddingFunction() -> Optional[EmbeddingFunction[Documents]]:
    if is_thin_client:
        return None
    else:
        return cast(
            EmbeddingFunction[Documents],
            # This is implicitly imported above
            ONNXMiniLM_L6_V2(),  # type: ignore[name-defined] # noqa: F821
        )


def get_builtins() -> Set[str]:
    return _all_classes
