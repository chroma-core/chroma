"""Minimal Chrome DevTools Protocol client built on stdlib only.

Just enough to:
  1. Open a WebSocket to the browser-level CDP endpoint advertised by
     Chrome's /json/version response.
  2. Send `Storage.getCookies` and read back the cookie list.

Not a general-purpose CDP client — no event subscriptions, no per-page
attach, no fragmentation, no permessage-deflate. Designed to be small
and dependency-free for the notion_cli login-chrome flow.

Reference:
  - DevTools Protocol HTTP endpoints:
    https://chromedevtools.github.io/devtools-protocol/
  - WebSocket framing: RFC 6455 (we implement just enough for short
    text frames sent and received in lock-step).
"""

from __future__ import annotations

import base64
import json
import os
import socket
import struct
import time
from typing import Any, Optional
from urllib.parse import urlparse


_OP_CONT = 0x0
_OP_TEXT = 0x1
_OP_BIN = 0x2
_OP_CLOSE = 0x8
_OP_PING = 0x9
_OP_PONG = 0xA


class WSError(RuntimeError):
    pass


class _MinimalWS:
    """Tiny WebSocket client. Single-threaded, blocking, text-only.

    Suitable for request/response style protocols where messages are
    short and the conversation is half-duplex. CDP fits.
    """

    def __init__(self, host: str, port: int, path: str, *, timeout: float = 10.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._buf = b""
        self._handshake(host, port, path)

    def _handshake(self, host: str, port: int, path: str) -> None:
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"\r\n"
        )
        self._sock.sendall(req.encode("ascii"))
        # Read response headers, leaving any leftover bytes in self._buf.
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise WSError("connection closed during WebSocket handshake")
            self._buf += chunk
        head, _, rest = self._buf.partition(b"\r\n\r\n")
        self._buf = rest
        status_line = head.split(b"\r\n", 1)[0].decode("latin-1", errors="replace")
        if " 101" not in status_line:
            raise WSError(f"bad WebSocket upgrade status: {status_line!r}")

    def send_text(self, text: str) -> None:
        payload = text.encode("utf-8")
        n = len(payload)
        first = 0x80 | _OP_TEXT  # FIN + text opcode
        if n < 126:
            header = struct.pack("!BB", first, 0x80 | n)
        elif n < 65536:
            header = struct.pack("!BBH", first, 0x80 | 126, n)
        else:
            header = struct.pack("!BBQ", first, 0x80 | 127, n)
        mask = os.urandom(4)
        masked = bytes(payload[i] ^ mask[i % 4] for i in range(n))
        self._sock.sendall(header + mask + masked)

    def recv_text(self) -> str:
        while True:
            opcode, payload = self._read_frame()
            if opcode == _OP_TEXT:
                return payload.decode("utf-8")
            if opcode == _OP_PING:
                self._send_control(_OP_PONG, payload)
                continue
            if opcode == _OP_PONG:
                continue
            if opcode == _OP_CLOSE:
                raise WSError("server sent CLOSE frame")
            # Binary / continuation: not expected from CDP. Skip.

    def _read_frame(self) -> tuple[int, bytes]:
        head = self._read_n(2)
        first, second = head[0], head[1]
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_n(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_n(8))[0]
        mask = self._read_n(4) if masked else None
        payload = self._read_n(length) if length else b""
        if mask:
            payload = bytes(payload[i] ^ mask[i % 4] for i in range(len(payload)))
        return opcode, payload

    def _send_control(self, opcode: int, payload: bytes) -> None:
        n = len(payload)
        if n > 125:
            payload = payload[:125]
            n = 125
        first = 0x80 | opcode
        mask = os.urandom(4)
        masked = bytes(payload[i] ^ mask[i % 4] for i in range(n))
        self._sock.sendall(struct.pack("!BB", first, 0x80 | n) + mask + masked)

    def _read_n(self, n: int) -> bytes:
        out = b""
        while len(out) < n:
            if self._buf:
                take = min(n - len(out), len(self._buf))
                out += self._buf[:take]
                self._buf = self._buf[take:]
                continue
            chunk = self._sock.recv(8192)
            if not chunk:
                raise WSError("connection closed mid-frame")
            self._buf += chunk
        return out

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass


class CDP:
    """Browser-level Chrome DevTools Protocol client. Built around the
    one method we care about: `Storage.getCookies`.
    """

    def __init__(self, ws_url: str, *, timeout: float = 10.0) -> None:
        u = urlparse(ws_url)
        host = u.hostname or "127.0.0.1"
        port = u.port or 9222
        path = u.path + (("?" + u.query) if u.query else "")
        self._ws = _MinimalWS(host, port, path, timeout=timeout)
        self._next_id = 1
        self._timeout = timeout

    def call(self, method: str, params: Optional[dict] = None, *, timeout: Optional[float] = None) -> dict:
        msg_id = self._next_id
        self._next_id += 1
        msg = {"id": msg_id, "method": method, "params": params or {}}
        self._ws.send_text(json.dumps(msg))
        deadline = time.time() + (timeout if timeout is not None else self._timeout)
        while time.time() < deadline:
            text = self._ws.recv_text()
            try:
                data = json.loads(text)
            except ValueError:
                continue
            if data.get("id") != msg_id:
                # Out-of-band event; ignore.
                continue
            if "error" in data:
                err = data["error"]
                raise RuntimeError(
                    f"CDP error on {method}: {err.get('code', '?')} "
                    f"{err.get('message', '?')}"
                )
            return data.get("result") or {}
        raise TimeoutError(f"no response to CDP call {method} within {timeout or self._timeout}s")

    def get_cookies(self) -> list[dict[str, Any]]:
        """Browser-level Storage.getCookies. Returns every cookie in the
        default browser context — we filter for notion.so on the caller
        side.
        """
        result = self.call("Storage.getCookies")
        return list(result.get("cookies") or [])

    def close(self) -> None:
        self._ws.close()
