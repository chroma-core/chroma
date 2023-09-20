# FAST API code
import logging
from typing import Optional, Dict, List, cast, Any

from overrides import override
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.types import ASGIApp

from chromadb.config import System
from chromadb.auth import (
    ServerAuthenticationRequest,
    AuthInfoType,
    ServerAuthenticationResponse,
    ServerAuthProvider,
    ChromaAuthMiddleware,
)
from chromadb.auth.registry import resolve_provider

logger = logging.getLogger(__name__)


class FastAPIServerAuthenticationRequest(ServerAuthenticationRequest[Optional[str]]):
    def __init__(self, request: Request) -> None:
        self._request = request

    @override
    def get_auth_info(
        self, auth_info_type: AuthInfoType, auth_info_id: Optional[str] = None
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

    @override
    def authenticate(
        self, request: ServerAuthenticationRequest[Any]
    ) -> Optional[ServerAuthenticationResponse]:
        return FastAPIServerAuthenticationResponse(
            self._auth_provider.authenticate(request)
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
        # We can potentially add an `/auth` endpoint to the server to allow for more complex auth flows
        return


class FastAPIChromaAuthMiddlewareWrapper(BaseHTTPMiddleware):  # type: ignore
    def __init__(
        self, app: ASGIApp, auth_middleware: FastAPIChromaAuthMiddleware
    ) -> None:
        super().__init__(app)
        self._middleware = auth_middleware
        self._middleware.instrument_server(app)

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
        return await call_next(request)
