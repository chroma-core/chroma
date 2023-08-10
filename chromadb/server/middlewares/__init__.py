from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

from chromadb.config import Settings


class SimpleTokenAuthMiddleware(BaseHTTPMiddleware):  # type: ignore
    """
    Very basic security middleware that checks for a token in the Authorization header
    """

    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        super().__init__(app)
        self.settings = settings
        if settings.chroma_server_middleware_token_auth_enabled:
            settings.require("chroma_server_middleware_token_auth_token")
            self.token = settings.chroma_server_middleware_token_auth_token

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Response]
    ) -> Response:
        # Extract the Authorization header
        auth_header = request.headers.get("Authorization", "").split()
        # Check if the header exists and the token is correct
        if len(auth_header) != 2 or auth_header[1] != self.token:
            return JSONResponse({"error": "Unauthorized"}, status_code=401)

        # If token is correct, continue to the next middleware or route handler
        response = await call_next(request)
        return response
