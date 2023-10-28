from functools import partial
from typing import Any, Callable, Dict
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
        elif type == AuthzResourceTypes.TENANT and isinstance(value, CreateTenant):
            return dict(value)
    return {}


def attr_from_resource_object(
    type: AuthzResourceTypes, **kwargs: Any
) -> Callable[..., Dict[str, Any]]:
    obj = find_key_with_value_of_type(type, **kwargs)
    return partial(lambda **kwargs: obj, **kwargs)
