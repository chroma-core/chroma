from typing import Any, Dict, Mapping, Optional, TypeVar
from urllib.parse import quote, urlparse, urlunparse
import logging
import orjson as json
import httpx

import chromadb.errors as errors
from chromadb.config import Component, Settings, System

logger = logging.getLogger(__name__)


# inherits from Component so that it can create an init function to use system
# this way it can build limits from the settings in System
class BaseHTTPClient(Component):
    _settings: Settings
    pre_flight_checks: Any = None
    DEFAULT_KEEPALIVE_SECS: float = 40.0

    def __init__(self, system: System):
        super().__init__(system)
        self._settings = system.settings
        keepalive_setting = self._settings.chroma_http_keepalive_secs
        self.keepalive_secs: Optional[float] = (
            keepalive_setting
            if keepalive_setting is not None
            else BaseHTTPClient.DEFAULT_KEEPALIVE_SECS
        )
        self._http_limits = self._build_limits()

    def _build_limits(self) -> httpx.Limits:
        limit_kwargs: Dict[str, Any] = {}
        if self.keepalive_secs is not None:
            limit_kwargs["keepalive_expiry"] = self.keepalive_secs

        max_connections = self._settings.chroma_http_max_connections
        if max_connections is not None:
            limit_kwargs["max_connections"] = max_connections

        max_keepalive_connections = self._settings.chroma_http_max_keepalive_connections
        if max_keepalive_connections is not None:
            limit_kwargs["max_keepalive_connections"] = max_keepalive_connections

        return httpx.Limits(**limit_kwargs)

    @property
    def http_limits(self) -> httpx.Limits:
        return self._http_limits

    @staticmethod
    def _validate_host(host: str) -> None:
        parsed = urlparse(host)
        if "/" in host and parsed.scheme not in {"http", "https"}:
            raise ValueError(
                "Invalid URL. " f"Unrecognized protocol - {parsed.scheme}."
            )
        if "/" in host and (not host.startswith("http")):
            raise ValueError(
                "Invalid URL. "
                "Seems that you are trying to pass URL as a host but without \
                  specifying the protocol. "
                "Please add http:// or https:// to the host."
            )

    @staticmethod
    def resolve_url(
        chroma_server_host: str,
        chroma_server_ssl_enabled: Optional[bool] = False,
        default_api_path: Optional[str] = "",
        chroma_server_http_port: Optional[int] = 8000,
    ) -> str:
        _skip_port = False
        _chroma_server_host = chroma_server_host
        BaseHTTPClient._validate_host(_chroma_server_host)
        if _chroma_server_host.startswith("http"):
            logger.debug("Skipping port as the user is passing a full URL")
            _skip_port = True
        parsed = urlparse(_chroma_server_host)

        scheme = "https" if chroma_server_ssl_enabled else parsed.scheme or "http"
        net_loc = parsed.netloc or parsed.hostname or chroma_server_host
        port = (
            ":" + str(parsed.port or chroma_server_http_port) if not _skip_port else ""
        )
        path = parsed.path or default_api_path

        if not path or path == net_loc:
            path = default_api_path if default_api_path else ""
        if not path.endswith(default_api_path or ""):
            path = path + default_api_path if default_api_path else ""
        full_url = urlunparse(
            (scheme, f"{net_loc}{port}", quote(path.replace("//", "/")), "", "", "")
        )

        return full_url

    # requests removes None values from the built query string, but httpx includes it as an empty value
    T = TypeVar("T", bound=Dict[Any, Any])

    @staticmethod
    def _clean_params(params: T) -> T:
        """Remove None values from provided dict."""
        return {k: v for k, v in params.items() if v is not None}  # type: ignore

    @staticmethod
    def _raise_chroma_error(resp: httpx.Response) -> None:
        """Raises an error if the response is not ok, using a ChromaError if possible."""
        try:
            resp.raise_for_status()
            return
        except httpx.HTTPStatusError:
            pass

        chroma_error = None
        try:
            body = json.loads(resp.text)
            if "error" in body:
                if body["error"] in errors.error_types:
                    chroma_error = errors.error_types[body["error"]](body["message"])

                    trace_id = resp.headers.get("chroma-trace-id")
                    if trace_id:
                        chroma_error.trace_id = trace_id

        except BaseException:
            pass

        if chroma_error:
            raise chroma_error

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError:
            trace_id = resp.headers.get("chroma-trace-id")
            if trace_id:
                raise Exception(f"{resp.text} (trace ID: {trace_id})")
            raise (Exception(resp.text))

    def get_request_headers(self) -> Mapping[str, str]:
        """Return headers used for HTTP requests."""
        return {}

    def get_api_url(self) -> str:
        """Return the API URL for this client."""
        return ""
