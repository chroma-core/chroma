from contextvars import ContextVar
from functools import wraps
import logging
from typing import Callable, Optional, Dict, List, Union, cast, Any
from starlette.requests import Request

import chromadb
from chromadb.config import DEFAULT_TENANT
from chromadb.auth import (
    AuthorizationContext,
    AuthzAction,
    AuthzResource,
    AuthzResourceActions,
    UserIdentity,
    DynamicAuthzResource,
    ServerAuthorizationProvider,
)
from chromadb.errors import AuthorizationError

logger = logging.getLogger(__name__)


request_var: ContextVar[Optional[Request]] = ContextVar("request_var",
                                                        default=None)
authz_provider: ContextVar[Optional[ServerAuthorizationProvider]] = ContextVar(
    "authz_provider", default=None
)


def authz_context(
    action: Union[str, AuthzResourceActions, List[str],
                  List[AuthzResourceActions]],
    resource: Union[AuthzResource, DynamicAuthzResource],
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(f)
        def wrapped(*args: Any, **kwargs: Dict[Any, Any]) -> Any:
            _dynamic_kwargs = {
                "api": args[0]._api,
                "function": f,
                "function_args": args,
                "function_kwargs": kwargs,
            }
            request = request_var.get()
            if not request:
                return

            _provider = authz_provider.get()
            a_list: List[Union[str, AuthzAction]] = []
            if not isinstance(action, list):
                a_list = [action]
            else:
                a_list = cast(List[Union[str, AuthzAction]], action)
            a_authz_responses = []
            for a in a_list:
                _action = a if isinstance(a, AuthzAction) else AuthzAction(id=a)
                _resource = (
                    resource
                    if isinstance(resource, AuthzResource)
                    else resource.to_authz_resource(**_dynamic_kwargs)
                )
                _context = AuthorizationContext(
                    user=UserIdentity(
                        user_id=request.state.user_identity.get_user_id()
                        if hasattr(request.state, "user_identity")
                        else "Anonymous",
                        tenant=request.state.user_identity.get_user_tenant()
                        if hasattr(request.state, "user_identity")
                        else DEFAULT_TENANT,
                        attributes=request.state.user_identity.get_user_attributes()
                        if hasattr(request.state, "user_identity")
                        else {},
                    ),
                    resource=_resource,
                    action=_action,
                )

                if _provider:
                    a_authz_responses.append(_provider.authorize(_context))
            if not any(a_authz_responses):
                raise AuthorizationError("Unauthorized")

            return f(*args, **kwargs)

        return wrapped

    return decorator
