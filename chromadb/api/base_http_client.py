from typing import Optional
from urllib.parse import quote, urlparse, urlunparse
import logging

from chromadb.config import Settings

logger = logging.getLogger(__name__)


class BaseHTTPClient:
    _settings: Settings
    _max_batch_size: int = -1

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
