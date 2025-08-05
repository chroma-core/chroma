# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Usage
-----

.. code-block:: python

    import fastapi
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    app = fastapi.FastAPI()

    @app.get("/foobar")
    async def foobar():
        return {"message": "hello world"}

    FastAPIInstrumentor.instrument_app(app)

Configuration
-------------

Exclude lists
*************
To exclude certain URLs from tracking, set the environment variable ``OTEL_PYTHON_FASTAPI_EXCLUDED_URLS``
(or ``OTEL_PYTHON_EXCLUDED_URLS`` to cover all instrumentations) to a string of comma delimited regexes that match the
URLs.

For example,

::

    export OTEL_PYTHON_FASTAPI_EXCLUDED_URLS="client/.*/info,healthcheck"

will exclude requests such as ``https://site/client/123/info`` and ``https://site/xyz/healthcheck``.

You can also pass comma delimited regexes directly to the ``instrument_app`` method:

.. code-block:: python

    FastAPIInstrumentor.instrument_app(app, excluded_urls="client/.*/info,healthcheck")

Request/Response hooks
**********************

This instrumentation supports request and response hooks. These are functions that get called
right after a span is created for a request and right before the span is finished for the response.

- The server request hook is passed a server span and ASGI scope object for every incoming request.
- The client request hook is called with the internal span, and ASGI scope and event when the method ``receive`` is called.
- The client response hook is called with the internal span, and ASGI scope and event when the method ``send`` is called.

.. code-block:: python

    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.trace import Span
    from typing import Any

    def server_request_hook(span: Span, scope: dict[str, Any]):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_request_hook", "some-value")

    def client_request_hook(span: Span, scope: dict[str, Any], message: dict[str, Any]):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_client_request_hook", "some-value")

    def client_response_hook(span: Span, scope: dict[str, Any], message: dict[str, Any]):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_response_hook", "some-value")

    FastAPIInstrumentor().instrument(server_request_hook=server_request_hook, client_request_hook=client_request_hook, client_response_hook=client_response_hook)

Capture HTTP request and response headers
*****************************************
You can configure the agent to capture specified HTTP headers as span attributes, according to the
`semantic convention <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-request-and-response-headers>`_.

Request headers
***************
To capture HTTP request headers as span attributes, set the environment variable
``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`` to a comma delimited list of HTTP header names,
or pass the ``http_capture_headers_server_request`` keyword argument to the ``instrument_app`` method.

For example using the environment variable,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST="content-type,custom_request_header"

will extract ``content-type`` and ``custom_request_header`` from the request headers and add them as span attributes.

Request header names in FastAPI are case-insensitive. So, giving the header name as ``CUStom-Header`` in the environment
variable will capture the header named ``custom-header``.

Regular expressions may also be used to match multiple headers that correspond to the given pattern.  For example:
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST="Accept.*,X-.*"

Would match all request headers that start with ``Accept`` and ``X-``.

To capture all request headers, set ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`` to ``".*"``.
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST=".*"

The name of the added span attribute will follow the format ``http.request.header.<header_name>`` where ``<header_name>``
is the normalized HTTP header name (lowercase, with ``-`` replaced by ``_``). The value of the attribute will be a
single item list containing all the header values.

For example:
``http.request.header.custom_request_header = ["<value1>", "<value2>"]``

Response headers
****************
To capture HTTP response headers as span attributes, set the environment variable
``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE`` to a comma delimited list of HTTP header names,
or pass the ``http_capture_headers_server_response`` keyword argument to the ``instrument_app`` method.

For example using the environment variable,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE="content-type,custom_response_header"

will extract ``content-type`` and ``custom_response_header`` from the response headers and add them as span attributes.

Response header names in FastAPI are case-insensitive. So, giving the header name as ``CUStom-Header`` in the environment
variable will capture the header named ``custom-header``.

Regular expressions may also be used to match multiple headers that correspond to the given pattern.  For example:
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE="Content.*,X-.*"

Would match all response headers that start with ``Content`` and ``X-``.

To capture all response headers, set ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE`` to ``".*"``.
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE=".*"

The name of the added span attribute will follow the format ``http.response.header.<header_name>`` where ``<header_name>``
is the normalized HTTP header name (lowercase, with ``-`` replaced by ``_``). The value of the attribute will be a
list containing the header values.

For example:
``http.response.header.custom_response_header = ["<value1>", "<value2>"]``

Sanitizing headers
******************
In order to prevent storing sensitive data such as personally identifiable information (PII), session keys, passwords,
etc, set the environment variable ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS``
to a comma delimited list of HTTP header names to be sanitized, or pass the ``http_capture_headers_sanitize_fields``
keyword argument to the ``instrument_app`` method.

Regexes may be used, and all header names will be matched in a case-insensitive manner.

For example using the environment variable,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS=".*session.*,set-cookie"

will replace the value of headers such as ``session-id`` and ``set-cookie`` with ``[REDACTED]`` in the span.

Note:
    The environment variable names used to capture HTTP headers are still experimental, and thus are subject to change.

API
---
"""

from __future__ import annotations

import functools
import logging
import types
from typing import Collection, Literal

import fastapi
from starlette.applications import Starlette
from starlette.middleware.errors import ServerErrorMiddleware
from starlette.routing import Match
from starlette.types import ASGIApp

from opentelemetry.instrumentation._semconv import (
    _get_schema_url,
    _OpenTelemetrySemanticConventionStability,
    _OpenTelemetryStabilitySignalType,
    _StabilityMode,
)
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.instrumentation.asgi.types import (
    ClientRequestHook,
    ClientResponseHook,
    ServerRequestHook,
)
from opentelemetry.instrumentation.fastapi.package import _instruments
from opentelemetry.instrumentation.fastapi.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.metrics import MeterProvider, get_meter
from opentelemetry.semconv.attributes.http_attributes import HTTP_ROUTE
from opentelemetry.trace import TracerProvider, get_tracer
from opentelemetry.util.http import (
    get_excluded_urls,
    parse_excluded_urls,
    sanitize_method,
)

_excluded_urls_from_env = get_excluded_urls("FASTAPI")
_logger = logging.getLogger(__name__)


class FastAPIInstrumentor(BaseInstrumentor):
    """An instrumentor for FastAPI

    See `BaseInstrumentor`
    """

    _original_fastapi = None

    @staticmethod
    def instrument_app(
        app: fastapi.FastAPI,
        server_request_hook: ServerRequestHook = None,
        client_request_hook: ClientRequestHook = None,
        client_response_hook: ClientResponseHook = None,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        excluded_urls: str | None = None,
        http_capture_headers_server_request: list[str] | None = None,
        http_capture_headers_server_response: list[str] | None = None,
        http_capture_headers_sanitize_fields: list[str] | None = None,
        exclude_spans: list[Literal["receive", "send"]] | None = None,
    ):
        """Instrument an uninstrumented FastAPI application.

        Args:
            app: The fastapi ASGI application callable to forward requests to.
            server_request_hook: Optional callback which is called with the server span and ASGI
                          scope object for every incoming request.
            client_request_hook: Optional callback which is called with the internal span, and ASGI
                          scope and event which are sent as dictionaries for when the method receive is called.
            client_response_hook: Optional callback which is called with the internal span, and ASGI
                          scope and event which are sent as dictionaries for when the method send is called.
            tracer_provider: The optional tracer provider to use. If omitted
                the current globally configured one is used.
            meter_provider: The optional meter provider to use. If omitted
                the current globally configured one is used.
            excluded_urls: Optional comma delimited string of regexes to match URLs that should not be traced.
            http_capture_headers_server_request: Optional list of HTTP headers to capture from the request.
            http_capture_headers_server_response: Optional list of HTTP headers to capture from the response.
            http_capture_headers_sanitize_fields: Optional list of HTTP headers to sanitize.
            exclude_spans: Optionally exclude HTTP `send` and/or `receive` spans from the trace.
        """
        if not hasattr(app, "_is_instrumented_by_opentelemetry"):
            app._is_instrumented_by_opentelemetry = False

        if not getattr(app, "_is_instrumented_by_opentelemetry", False):
            # initialize semantic conventions opt-in if needed
            _OpenTelemetrySemanticConventionStability._initialize()
            sem_conv_opt_in_mode = _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
                _OpenTelemetryStabilitySignalType.HTTP,
            )
            if excluded_urls is None:
                excluded_urls = _excluded_urls_from_env
            else:
                excluded_urls = parse_excluded_urls(excluded_urls)
            tracer = get_tracer(
                __name__,
                __version__,
                tracer_provider,
                schema_url=_get_schema_url(sem_conv_opt_in_mode),
            )
            meter = get_meter(
                __name__,
                __version__,
                meter_provider,
                schema_url=_get_schema_url(sem_conv_opt_in_mode),
            )

            # Instead of using `app.add_middleware` we monkey patch `build_middleware_stack` to insert our middleware
            # as the outermost middleware.
            # Otherwise `OpenTelemetryMiddleware` would have unhandled exceptions tearing through it and would not be able
            # to faithfully record what is returned to the client since it technically cannot know what `ServerErrorMiddleware` is going to do.

            def build_middleware_stack(self: Starlette) -> ASGIApp:
                inner_server_error_middleware: ASGIApp = (  # type: ignore
                    self._original_build_middleware_stack()  # type: ignore
                )
                otel_middleware = OpenTelemetryMiddleware(
                    inner_server_error_middleware,
                    excluded_urls=excluded_urls,
                    default_span_details=_get_default_span_details,
                    server_request_hook=server_request_hook,
                    client_request_hook=client_request_hook,
                    client_response_hook=client_response_hook,
                    # Pass in tracer/meter to get __name__and __version__ of fastapi instrumentation
                    tracer=tracer,
                    meter=meter,
                    http_capture_headers_server_request=http_capture_headers_server_request,
                    http_capture_headers_server_response=http_capture_headers_server_response,
                    http_capture_headers_sanitize_fields=http_capture_headers_sanitize_fields,
                    exclude_spans=exclude_spans,
                )
                # Wrap in an outer layer of ServerErrorMiddleware so that any exceptions raised in OpenTelemetryMiddleware
                # are handled.
                # This should not happen unless there is a bug in OpenTelemetryMiddleware, but if there is we don't want that
                # to impact the user's application just because we wrapped the middlewares in this order.
                if isinstance(
                    inner_server_error_middleware, ServerErrorMiddleware
                ):  # usually true
                    outer_server_error_middleware = ServerErrorMiddleware(
                        app=otel_middleware,
                    )
                else:
                    # Something else seems to have patched things, or maybe Starlette changed.
                    # Just create a default ServerErrorMiddleware.
                    outer_server_error_middleware = ServerErrorMiddleware(
                        app=otel_middleware
                    )
                return outer_server_error_middleware

            app._original_build_middleware_stack = app.build_middleware_stack
            app.build_middleware_stack = types.MethodType(
                functools.wraps(app.build_middleware_stack)(
                    build_middleware_stack
                ),
                app,
            )

            app._is_instrumented_by_opentelemetry = True
            if app not in _InstrumentedFastAPI._instrumented_fastapi_apps:
                _InstrumentedFastAPI._instrumented_fastapi_apps.add(app)
        else:
            _logger.warning(
                "Attempting to instrument FastAPI app while already instrumented"
            )

    @staticmethod
    def uninstrument_app(app: fastapi.FastAPI):
        original_build_middleware_stack = getattr(
            app, "_original_build_middleware_stack", None
        )
        if original_build_middleware_stack:
            app.build_middleware_stack = original_build_middleware_stack
            del app._original_build_middleware_stack
        app.middleware_stack = app.build_middleware_stack()
        app._is_instrumented_by_opentelemetry = False

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        self._original_fastapi = fastapi.FastAPI
        _InstrumentedFastAPI._tracer_provider = kwargs.get("tracer_provider")
        _InstrumentedFastAPI._server_request_hook = kwargs.get(
            "server_request_hook"
        )
        _InstrumentedFastAPI._client_request_hook = kwargs.get(
            "client_request_hook"
        )
        _InstrumentedFastAPI._client_response_hook = kwargs.get(
            "client_response_hook"
        )
        _InstrumentedFastAPI._http_capture_headers_server_request = kwargs.get(
            "http_capture_headers_server_request"
        )
        _InstrumentedFastAPI._http_capture_headers_server_response = (
            kwargs.get("http_capture_headers_server_response")
        )
        _InstrumentedFastAPI._http_capture_headers_sanitize_fields = (
            kwargs.get("http_capture_headers_sanitize_fields")
        )
        _InstrumentedFastAPI._excluded_urls = kwargs.get("excluded_urls")
        _InstrumentedFastAPI._meter_provider = kwargs.get("meter_provider")
        _InstrumentedFastAPI._exclude_spans = kwargs.get("exclude_spans")
        fastapi.FastAPI = _InstrumentedFastAPI

    def _uninstrument(self, **kwargs):
        for instance in _InstrumentedFastAPI._instrumented_fastapi_apps:
            self.uninstrument_app(instance)
        _InstrumentedFastAPI._instrumented_fastapi_apps.clear()
        fastapi.FastAPI = self._original_fastapi


class _InstrumentedFastAPI(fastapi.FastAPI):
    _tracer_provider = None
    _meter_provider = None
    _excluded_urls = None
    _server_request_hook: ServerRequestHook = None
    _client_request_hook: ClientRequestHook = None
    _client_response_hook: ClientResponseHook = None
    _http_capture_headers_server_request: list[str] | None = None
    _http_capture_headers_server_response: list[str] | None = None
    _http_capture_headers_sanitize_fields: list[str] | None = None
    _exclude_spans: list[Literal["receive", "send"]] | None = None

    _instrumented_fastapi_apps = set()
    _sem_conv_opt_in_mode = _StabilityMode.DEFAULT

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        FastAPIInstrumentor.instrument_app(
            self,
            server_request_hook=self._server_request_hook,
            client_request_hook=self._client_request_hook,
            client_response_hook=self._client_response_hook,
            tracer_provider=self._tracer_provider,
            meter_provider=self._meter_provider,
            excluded_urls=self._excluded_urls,
            http_capture_headers_server_request=self._http_capture_headers_server_request,
            http_capture_headers_server_response=self._http_capture_headers_server_response,
            http_capture_headers_sanitize_fields=self._http_capture_headers_sanitize_fields,
            exclude_spans=self._exclude_spans,
        )
        _InstrumentedFastAPI._instrumented_fastapi_apps.add(self)

    def __del__(self):
        if self in _InstrumentedFastAPI._instrumented_fastapi_apps:
            _InstrumentedFastAPI._instrumented_fastapi_apps.remove(self)


def _get_route_details(scope):
    """
    Function to retrieve Starlette route from scope.

    TODO: there is currently no way to retrieve http.route from
    a starlette application from scope.
    See: https://github.com/encode/starlette/pull/804

    Args:
        scope: A Starlette scope
    Returns:
        A string containing the route or None
    """
    app = scope["app"]
    route = None

    for starlette_route in app.routes:
        match, _ = starlette_route.matches(scope)
        if match == Match.FULL:
            route = starlette_route.path
            break
        if match == Match.PARTIAL:
            route = starlette_route.path
    return route


def _get_default_span_details(scope):
    """
    Callback to retrieve span name and attributes from scope.

    Args:
        scope: A Starlette scope
    Returns:
        A tuple of span name and attributes
    """
    route = _get_route_details(scope)
    method = sanitize_method(scope.get("method", "").strip())
    attributes = {}
    if method == "_OTHER":
        method = "HTTP"
    if route:
        attributes[HTTP_ROUTE] = route
    if method and route:  # http
        span_name = f"{method} {route}"
    elif route:  # websocket
        span_name = route
    else:  # fallback
        span_name = method
    return span_name, attributes
