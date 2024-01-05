from functools import partial
from typing import Any, Callable, Dict, cast
from chromadb.api import ServerAPI


def attr_from_collection_lookup(
    collection_id_arg: str, **kwargs: Any
) -> Callable[..., Dict[str, Any]]:
    def _wrap(**kwargs: Any) -> Dict[str, Any]:
        _api = cast(ServerAPI, kwargs["api"])
        col = _api.get_collection(
            id=kwargs["function_kwargs"][collection_id_arg])
        return {"tenant": col.tenant, "database": col.database}

    return partial(_wrap, **kwargs)
