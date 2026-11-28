import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import MagicMock, patch

import httpx

import chromadb
from chromadb.api import ServerAPI
from chromadb.api.client import Client
from chromadb.api.fastapi import FastAPI
from chromadb.api.shared_system_client import SharedSystemClient
from chromadb.config import Settings, System


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        payload: dict[str, Any]
        if path.endswith("/auth/identity"):
            payload = {"user_id": "user"}
        elif path.endswith("/heartbeat"):
            payload = {"nanosecond heartbeat": 1}
        elif path.endswith("/databases/default_database"):
            payload = {
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "default_database",
                "tenant": "default_tenant",
            }
        elif path.startswith("/api/v2/tenants/"):
            payload = {"name": "default_tenant"}
        else:
            body = f"unexpected path {path}".encode()
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def test_http_client_close_closes_owned_session() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = int(server.server_address[1])
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    sessions: list[httpx.Client] = []
    try:
        client = chromadb.HttpClient(host="127.0.0.1", port=port)
        assert isinstance(client, Client)
        with client:
            client.heartbeat()
            for system in list(SharedSystemClient._identifier_to_system.values()):
                api = system.instance(ServerAPI)
                if isinstance(api, FastAPI):
                    sessions.append(api._session)
        assert sessions
        assert all(session.is_closed for session in sessions)
    finally:
        server.shutdown()
        server.server_close()
        for session in sessions:
            if not session.is_closed:
                session.close()
        SharedSystemClient.clear_system_cache()


def test_fastapi_stop_does_not_close_replaced_session() -> None:
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="127.0.0.1",
        chroma_server_http_port=9,
    )
    system = System(settings)
    with patch.object(FastAPI, "require", side_effect=[MagicMock(), MagicMock()]):
        api = FastAPI(system)
    owned = api._session
    external = httpx.Client()
    api._session = external
    try:
        api.stop()
        assert owned.is_closed
        assert external.is_closed is False
    finally:
        external.close()
        if not owned.is_closed:
            owned.close()
