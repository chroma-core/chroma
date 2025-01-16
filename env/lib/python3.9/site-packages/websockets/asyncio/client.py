from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Generator, Sequence

from ..client import ClientProtocol
from ..datastructures import HeadersLike
from ..extensions.base import ClientExtensionFactory
from ..extensions.permessage_deflate import enable_client_permessage_deflate
from ..headers import validate_subprotocols
from ..http11 import USER_AGENT, Response
from ..protocol import CONNECTING, Event
from ..typing import LoggerLike, Origin, Subprotocol
from ..uri import parse_uri
from .compatibility import TimeoutError, asyncio_timeout
from .connection import Connection


__all__ = ["connect", "unix_connect", "ClientConnection"]


class ClientConnection(Connection):
    """
    :mod:`asyncio` implementation of a WebSocket client connection.

    :class:`ClientConnection` provides :meth:`recv` and :meth:`send` coroutines
    for receiving and sending messages.

    It supports asynchronous iteration to receive messages::

        async for message in websocket:
            await process(message)

    The iterator exits normally when the connection is closed with close code
    1000 (OK) or 1001 (going away) or without a close code. It raises a
    :exc:`~websockets.exceptions.ConnectionClosedError` when the connection is
    closed with any other code.

    The ``ping_interval``, ``ping_timeout``, ``close_timeout``, ``max_queue``,
    and ``write_limit`` arguments the same meaning as in :func:`connect`.

    Args:
        protocol: Sans-I/O connection.

    """

    def __init__(
        self,
        protocol: ClientProtocol,
        *,
        ping_interval: float | None = 20,
        ping_timeout: float | None = 20,
        close_timeout: float | None = 10,
        max_queue: int | tuple[int, int | None] = 16,
        write_limit: int | tuple[int, int | None] = 2**15,
    ) -> None:
        self.protocol: ClientProtocol
        super().__init__(
            protocol,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            close_timeout=close_timeout,
            max_queue=max_queue,
            write_limit=write_limit,
        )
        self.response_rcvd: asyncio.Future[None] = self.loop.create_future()

    async def handshake(
        self,
        additional_headers: HeadersLike | None = None,
        user_agent_header: str | None = USER_AGENT,
    ) -> None:
        """
        Perform the opening handshake.

        """
        async with self.send_context(expected_state=CONNECTING):
            self.request = self.protocol.connect()
            if additional_headers is not None:
                self.request.headers.update(additional_headers)
            if user_agent_header:
                self.request.headers["User-Agent"] = user_agent_header
            self.protocol.send_request(self.request)

        # May raise CancelledError if open_timeout is exceeded.
        await self.response_rcvd

        if self.response is None:
            raise ConnectionError("connection closed during handshake")

        if self.protocol.handshake_exc is None:
            self.start_keepalive()
        else:
            try:
                async with asyncio_timeout(self.close_timeout):
                    await self.connection_lost_waiter
            finally:
                raise self.protocol.handshake_exc

    def process_event(self, event: Event) -> None:
        """
        Process one incoming event.

        """
        # First event - handshake response.
        if self.response is None:
            assert isinstance(event, Response)
            self.response = event
            self.response_rcvd.set_result(None)
        # Later events - frames.
        else:
            super().process_event(event)

    def connection_lost(self, exc: Exception | None) -> None:
        try:
            super().connection_lost(exc)
        finally:
            # If the connection is closed during the handshake, unblock it.
            if not self.response_rcvd.done():
                self.response_rcvd.set_result(None)


# This is spelled in lower case because it's exposed as a callable in the API.
class connect:
    """
    Connect to the WebSocket server at ``uri``.

    This coroutine returns a :class:`ClientConnection` instance, which you can
    use to send and receive messages.

    :func:`connect` may be used as an asynchronous context manager::

        async with websockets.asyncio.client.connect(...) as websocket:
            ...

    The connection is closed automatically when exiting the context.

    Args:
        uri: URI of the WebSocket server.
        origin: Value of the ``Origin`` header, for servers that require it.
        extensions: List of supported extensions, in order in which they
            should be negotiated and run.
        subprotocols: List of supported subprotocols, in order of decreasing
            preference.
        additional_headers (HeadersLike | None): Arbitrary HTTP headers to add
            to the handshake request.
        user_agent_header: Value of  the ``User-Agent`` request header.
            It defaults to ``"Python/x.y.z websockets/X.Y"``.
            Setting it to :obj:`None` removes the header.
        compression: The "permessage-deflate" extension is enabled by default.
            Set ``compression`` to :obj:`None` to disable it. See the
            :doc:`compression guide <../../topics/compression>` for details.
        open_timeout: Timeout for opening the connection in seconds.
            :obj:`None` disables the timeout.
        ping_interval: Interval between keepalive pings in seconds.
            :obj:`None` disables keepalive.
        ping_timeout: Timeout for keepalive pings in seconds.
            :obj:`None` disables timeouts.
        close_timeout: Timeout for closing the connection in seconds.
            :obj:`None` disables the timeout.
        max_size: Maximum size of incoming messages in bytes.
            :obj:`None` disables the limit.
        max_queue: High-water mark of the buffer where frames are received.
            It defaults to 16 frames. The low-water mark defaults to ``max_queue
            // 4``. You may pass a ``(high, low)`` tuple to set the high-water
            and low-water marks.
        write_limit: High-water mark of write buffer in bytes. It is passed to
            :meth:`~asyncio.WriteTransport.set_write_buffer_limits`. It defaults
            to 32 KiB. You may pass a ``(high, low)`` tuple to set the
            high-water and low-water marks.
        logger: Logger for this client.
            It defaults to ``logging.getLogger("websockets.client")``.
            See the :doc:`logging guide <../../topics/logging>` for details.
        create_connection: Factory for the :class:`ClientConnection` managing
            the connection. Set it to a wrapper or a subclass to customize
            connection handling.

    Any other keyword arguments are passed to the event loop's
    :meth:`~asyncio.loop.create_connection` method.

    For example:

    * You can set ``ssl`` to a :class:`~ssl.SSLContext` to enforce TLS settings.
      When connecting to a ``wss://`` URI, if ``ssl`` isn't provided, a TLS
      context is created with :func:`~ssl.create_default_context`.

    * You can set ``server_hostname`` to override the host name from ``uri`` in
      the TLS handshake.

    * You can set ``host`` and ``port`` to connect to a different host and port
      from those found in ``uri``. This only changes the destination of the TCP
      connection. The host name from ``uri`` is still used in the TLS handshake
      for secure connections and in the ``Host`` header.

    * You can set ``sock`` to provide a preexisting TCP socket. You may call
      :func:`socket.create_connection` (not to be confused with the event loop's
      :meth:`~asyncio.loop.create_connection` method) to create a suitable
      client socket and customize it.

    Raises:
        InvalidURI: If ``uri`` isn't a valid WebSocket URI.
        OSError: If the TCP connection fails.
        InvalidHandshake: If the opening handshake fails.
        TimeoutError: If the opening handshake times out.

    """

    def __init__(
        self,
        uri: str,
        *,
        # WebSocket
        origin: Origin | None = None,
        extensions: Sequence[ClientExtensionFactory] | None = None,
        subprotocols: Sequence[Subprotocol] | None = None,
        additional_headers: HeadersLike | None = None,
        user_agent_header: str | None = USER_AGENT,
        compression: str | None = "deflate",
        # Timeouts
        open_timeout: float | None = 10,
        ping_interval: float | None = 20,
        ping_timeout: float | None = 20,
        close_timeout: float | None = 10,
        # Limits
        max_size: int | None = 2**20,
        max_queue: int | tuple[int, int | None] = 16,
        write_limit: int | tuple[int, int | None] = 2**15,
        # Logging
        logger: LoggerLike | None = None,
        # Escape hatch for advanced customization
        create_connection: type[ClientConnection] | None = None,
        # Other keyword arguments are passed to loop.create_connection
        **kwargs: Any,
    ) -> None:

        wsuri = parse_uri(uri)

        if wsuri.secure:
            kwargs.setdefault("ssl", True)
            kwargs.setdefault("server_hostname", wsuri.host)
            if kwargs.get("ssl") is None:
                raise TypeError("ssl=None is incompatible with a wss:// URI")
        else:
            if kwargs.get("ssl") is not None:
                raise TypeError("ssl argument is incompatible with a ws:// URI")

        if subprotocols is not None:
            validate_subprotocols(subprotocols)

        if compression == "deflate":
            extensions = enable_client_permessage_deflate(extensions)
        elif compression is not None:
            raise ValueError(f"unsupported compression: {compression}")

        if create_connection is None:
            create_connection = ClientConnection

        def factory() -> ClientConnection:
            # This is a protocol in the Sans-I/O implementation of websockets.
            protocol = ClientProtocol(
                wsuri,
                origin=origin,
                extensions=extensions,
                subprotocols=subprotocols,
                max_size=max_size,
                logger=logger,
            )
            # This is a connection in websockets and a protocol in asyncio.
            connection = create_connection(
                protocol,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                close_timeout=close_timeout,
                max_queue=max_queue,
                write_limit=write_limit,
            )
            return connection

        loop = asyncio.get_running_loop()
        if kwargs.pop("unix", False):
            self._create_connection = loop.create_unix_connection(factory, **kwargs)
        else:
            if kwargs.get("sock") is None:
                kwargs.setdefault("host", wsuri.host)
                kwargs.setdefault("port", wsuri.port)
            self._create_connection = loop.create_connection(factory, **kwargs)

        self._handshake_args = (
            additional_headers,
            user_agent_header,
        )

        self._open_timeout = open_timeout

    # async with connect(...) as ...: ...

    async def __aenter__(self) -> ClientConnection:
        return await self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.connection.close()

    # ... = await connect(...)

    def __await__(self) -> Generator[Any, None, ClientConnection]:
        # Create a suitable iterator by calling __await__ on a coroutine.
        return self.__await_impl__().__await__()

    async def __await_impl__(self) -> ClientConnection:
        try:
            async with asyncio_timeout(self._open_timeout):
                _transport, self.connection = await self._create_connection
                try:
                    await self.connection.handshake(*self._handshake_args)
                except (Exception, asyncio.CancelledError):
                    self.connection.transport.close()
                    raise
                else:
                    return self.connection
        except TimeoutError:
            # Re-raise exception with an informative error message.
            raise TimeoutError("timed out during handshake") from None

    # ... = yield from connect(...) - remove when dropping Python < 3.10

    __iter__ = __await__


def unix_connect(
    path: str | None = None,
    uri: str | None = None,
    **kwargs: Any,
) -> connect:
    """
    Connect to a WebSocket server listening on a Unix socket.

    This function accepts the same keyword arguments as :func:`connect`.

    It's only available on Unix.

    It's mainly useful for debugging servers listening on Unix sockets.

    Args:
        path: File system path to the Unix socket.
        uri: URI of the WebSocket server. ``uri`` defaults to
            ``ws://localhost/`` or, when a ``ssl`` argument is provided, to
            ``wss://localhost/``.

    """
    if uri is None:
        if kwargs.get("ssl") is None:
            uri = "ws://localhost/"
        else:
            uri = "wss://localhost/"
    return connect(uri=uri, unix=True, path=path, **kwargs)
