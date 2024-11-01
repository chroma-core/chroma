from typing import Any, Awaitable, Callable, Dict, List, Optional
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry import trace


def instrument_fastapi(app: FastAPI, excluded_urls: Optional[List[str]] = None) -> None:
    """Instrument FastAPI to emit OpenTelemetry spans."""

    # FastAPI calls middleware in reverse order, we want our filter disconnect
    # middleware to be called first to ensure that we have a chance to mark
    # spans as disconnected before they are ended by the instrumentation
    # middleware.
    app.add_middleware(FilterDisconnectMiddleware)
    FastAPIInstrumentor.instrument_app(
        app, excluded_urls=",".join(excluded_urls) if excluded_urls else None
    )


class FilterDisconnectMiddleware:
    def __init__(self, app: Any):
        self.app = app

    async def __call__(
        self,
        scope: Dict[str, Any],
        receive: Callable[[], Awaitable[Dict[str, Any]]],
        send: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_span = trace.get_current_span()

        async def process() -> Dict[str, Any]:
            message = await receive()
            if message["type"] == "http.disconnect":
                request_span.set_attribute("http.disconnect", True)

            return message

        await self.app(scope, process, send)
