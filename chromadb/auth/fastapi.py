# FAST API code
from contextvars import ContextVar
from functools import wraps
import logging
from typing import Callable, Optional, Dict, List, Union, cast, Any
from overrides import override
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.types import ASGIApp

from chromadb.config import DEFAULT_TENANT, System
from chromadb.auth import (
    AuthorizationContext,
    AuthorizationError,
    AuthorizationRequestContext,
    AuthzAction,
    AuthzResource,
    AuthzResourceActions,
    AuthzUser,
    DynamicAuthzResource,
    ServerAuthenticationRequest,
    AuthInfoType,
    ServerAuthenticationResponse,
    ServerAuthProvider,
    ChromaAuthMiddleware,
    ChromaAuthzMiddleware,
    ServerAuthorizationProvider,
)
from chromadb.auth.registry import resolve_provider
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)

logger = logging.getLogger(__name__)


class FastAPIServerAuthenticationRequest(ServerAuthenticationRequest[Optional[str]]):
    def __init__(self, request: Request) -> None:
        self._request = request

    @override
    def get_auth_info(
        self, auth_info_type: AuthInfoType, auth_info_id: str
    ) -> Optional[str]:
        if auth_info_type == AuthInfoType.HEADER:
            return str(self._request.headers[auth_info_id])
        elif auth_info_type == AuthInfoType.COOKIE:
            return str(self._request.cookies[auth_info_id])
        elif auth_info_type == AuthInfoType.URL:
            return str(self._request.query_params[auth_info_id])
        elif auth_info_type == AuthInfoType.METADATA:
            raise ValueError("Metadata not supported for FastAPI")
        else:
            raise ValueError(f"Unknown auth info type: {auth_info_type}")


class FastAPIServerAuthenticationResponse(ServerAuthenticationResponse):
    _auth_success: bool

    def __init__(self, auth_success: bool) -> None:
        self._auth_success = auth_success

    @override
    def success(self) -> bool:
        return self._auth_success


class FastAPIChromaAuthMiddleware(ChromaAuthMiddleware):
    _auth_provider: ServerAuthProvider

    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._system = system
        self._settings = system.settings
        self._settings.require("chroma_server_auth_provider")
        self._ignore_auth_paths: Dict[
            str, List[str]
        ] = self._settings.chroma_server_auth_ignore_paths
        if self._settings.chroma_server_auth_provider:
            logger.debug(
                f"Server Auth Provider: {self._settings.chroma_server_auth_provider}"
            )
            _cls = resolve_provider(
                self._settings.chroma_server_auth_provider, ServerAuthProvider
            )
            self._auth_provider = cast(ServerAuthProvider, self.require(_cls))

    @trace_method(
        "FastAPIChromaAuthMiddleware.authenticate", OpenTelemetryGranularity.ALL
    )
    @override
    def authenticate(
        self, request: ServerAuthenticationRequest[Any]
    ) -> ServerAuthenticationResponse:
        return self._auth_provider.authenticate(request)

    @trace_method(
        "FastAPIChromaAuthMiddleware.ignore_operation", OpenTelemetryGranularity.ALL
    )
    @override
    def ignore_operation(self, verb: str, path: str) -> bool:
        if (
            path in self._ignore_auth_paths.keys()
            and verb.upper() in self._ignore_auth_paths[path]
        ):
            logger.debug(f"Skipping auth for path {path} and method {verb}")
            return True
        return False

    @override
    def instrument_server(self, app: ASGIApp) -> None:
        # We can potentially add an `/auth` endpoint to the server to allow for more
        # complex auth flows
        raise NotImplementedError("Not implemented yet")


class FastAPIChromaAuthMiddlewareWrapper(BaseHTTPMiddleware):  # type: ignore
    def __init__(
        self, app: ASGIApp, auth_middleware: FastAPIChromaAuthMiddleware
    ) -> None:
        super().__init__(app)
        self._middleware = auth_middleware
        try:
            self._middleware.instrument_server(app)
        except NotImplementedError:
            pass

    @trace_method(
        "FastAPIChromaAuthMiddlewareWrapper.dispatch", OpenTelemetryGranularity.ALL
    )
    @override
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if self._middleware.ignore_operation(request.method, request.url.path):
            logger.debug(
                f"Skipping auth for path {request.url.path} and method {request.method}"
            )
            return await call_next(request)
        response = self._middleware.authenticate(
            FastAPIServerAuthenticationRequest(request)
        )
        if not response or not response.success():
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        request.state.user_identity = response.get_user_identity()
        return await call_next(request)


request_var: ContextVar[Optional[Request]] = ContextVar("request_var", default=None)
authz_provider: ContextVar[Optional[ServerAuthorizationProvider]] = ContextVar(
    "authz_provider", default=None
)


def authz_context(
    action: Union[str, AuthzResourceActions, List[str], List[AuthzResourceActions]],
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
            if request:
                _provider = authz_provider.get()
                a_list: List[Union[str, AuthzAction]] = []
                if not isinstance(action, List):
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
                        user=AuthzUser(
                            id=request.state.user_identity.get_user_id()
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


class FastAPIAuthorizationRequestContext(AuthorizationRequestContext[Request]):
    _request: Request

    def __init__(self, request: Request) -> None:
        self._request = request
        pass

    @override
    def get_request(self) -> Request:
        return self._request


class FastAPIChromaAuthzMiddleware(ChromaAuthzMiddleware[ASGIApp, Request]):
    _authz_provider: ServerAuthorizationProvider

    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._system = system
        self._settings = system.settings
        self._settings.require("chroma_server_authz_provider")
        self._ignore_auth_paths: Dict[
            str, List[str]
        ] = self._settings.chroma_server_authz_ignore_paths
        if self._settings.chroma_server_authz_provider:
            logger.debug(
                "Server Authorization Provider: "
                f"{self._settings.chroma_server_authz_provider}"
            )
            _cls = resolve_provider(
                self._settings.chroma_server_authz_provider, ServerAuthorizationProvider
            )
            self._authz_provider = cast(ServerAuthorizationProvider, self.require(_cls))

    @override
    def pre_process(self, request: AuthorizationRequestContext[Request]) -> None:
        rest_request = request.get_request()
        request_var.set(rest_request)
        authz_provider.set(self._authz_provider)

    @override
    def ignore_operation(self, verb: str, path: str) -> bool:
        if (
            path in self._ignore_auth_paths.keys()
            and verb.upper() in self._ignore_auth_paths[path]
        ):
            logger.debug(f"Skipping authz for path {path} and method {verb}")
            return True
        return False

    @override
    def instrument_server(self, app: ASGIApp) -> None:
        # We can potentially add an `/auth` endpoint to the server to allow
        # for more complex auth flows
        raise NotImplementedError("Not implemented yet")


class FastAPIChromaAuthzMiddlewareWrapper(BaseHTTPMiddleware):  # type: ignore
    def __init__(
        self, app: ASGIApp, authz_middleware: FastAPIChromaAuthzMiddleware
    ) -> None:
        super().__init__(app)
        self._middleware = authz_middleware
        try:
            self._middleware.instrument_server(app)
        except NotImplementedError:
            pass

    @trace_method(
        "FastAPIChromaAuthzMiddlewareWrapper.dispatch", OpenTelemetryGranularity.ALL
    )
    @override
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if self._middleware.ignore_operation(request.method, request.url.path):
            logger.debug(
                f"Skipping authz for path {request.url.path} "
                "and method {request.method}"
            )
            return await call_next(request)
        self._middleware.pre_process(FastAPIAuthorizationRequestContext(request))
        return await call_next(request)
