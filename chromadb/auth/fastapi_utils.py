from functools import partial
from typing import Any, Callable, Dict, Optional, Sequence, cast
from chromadb.utils.fastapi import string_to_uuid
from chromadb.api import ServerAPI
from chromadb.auth import AuthzResourceTypes


def find_key_with_value_of_type(
    type: AuthzResourceTypes, **kwargs: Any
) -> Dict[str, Any]:
    from chromadb.server.fastapi.types import (
        CreateCollection,
        CreateDatabase,
        CreateTenant,
    )

    for key, value in kwargs.items():
        if type == AuthzResourceTypes.DB and isinstance(value, CreateDatabase):
            return dict(value)
        elif type == AuthzResourceTypes.COLLECTION and isinstance(
            value, CreateCollection
        ):
            return dict(value)
        elif type == AuthzResourceTypes.TENANT and isinstance(value,
                                                              CreateTenant):
            return dict(value)
    return {}


def attr_from_resource_object(
    type: AuthzResourceTypes,
    additional_attrs: Optional[Sequence[str]] = None,
    **kwargs: Any,
) -> Callable[..., Dict[str, Any]]:
    def _wrap(**wkwargs: Any) -> Dict[str, Any]:
        obj = find_key_with_value_of_type(type, **wkwargs)
        if additional_attrs:
            obj.update({k: wkwargs["function_kwargs"][k]
                       for k in additional_attrs})
        return obj

    return partial(_wrap, **kwargs)


def attr_from_collection_lookup(
    collection_id_arg: str, **kwargs: Any
) -> Callable[..., Dict[str, Any]]:
    def _wrap(**kwargs: Any) -> Dict[str, Any]:
        _api = cast(ServerAPI, kwargs["api"])
        col = _api.get_collection(
            id=string_to_uuid(kwargs["function_kwargs"][collection_id_arg]))
        return {"tenant": col.tenant, "database": col.database}

    return partial(_wrap, **kwargs)
