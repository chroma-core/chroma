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
# pylint: disable=too-many-locals

"""
The opentelemetry-instrumentation-asgi package provides an ASGI middleware that can be used
on any ASGI framework (such as Django-channels / Quart) to track request timing through OpenTelemetry.

Usage (Quart)
-------------

.. code-block:: python

    from quart import Quart
    from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware

    app = Quart(__name__)
    app.asgi_app = OpenTelemetryMiddleware(app.asgi_app)

    @app.route("/")
    async def hello():
        return "Hello!"

    if __name__ == "__main__":
        app.run(debug=True)


Usage (Django 3.0)
------------------

Modify the application's ``asgi.py`` file as shown below.

.. code-block:: python

    import os
    from django.core.asgi import get_asgi_application
    from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'asgi_example.settings')

    application = get_asgi_application()
    application = OpenTelemetryMiddleware(application)


Usage (Raw ASGI)
----------------

.. code-block:: python

    from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware

    app = ...  # An ASGI application.
    app = OpenTelemetryMiddleware(app)


Configuration
-------------

Request/Response hooks
**********************

This instrumentation supports request and response hooks. These are functions that get called
right after a span is created for a request and right before the span is finished for the response.

- The server request hook is passed a server span and ASGI scope object for every incoming request.
- The client request hook is called with the internal span and an ASGI scope when the method ``receive`` is called.
- The client response hook is called with the internal span and an ASGI event when the method ``send`` is called.

For example,

.. code-block:: python

    def server_request_hook(span: Span, scope: dict[str, Any]):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_request_hook", "some-value")

    def client_request_hook(span: Span, scope: dict[str, Any], message: dict[str, Any]):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_client_request_hook", "some-value")

    def client_response_hook(span: Span, scope: dict[str, Any], message: dict[str, Any]):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_response_hook", "some-value")

   OpenTelemetryMiddleware().(application, server_request_hook=server_request_hook, client_request_hook=client_request_hook, client_response_hook=client_response_hook)

Capture HTTP request and response headers
*****************************************
You can configure the agent to capture specified HTTP headers as span attributes, according to the
`semantic convention <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-request-and-response-headers>`_.

Request headers
***************
To capture HTTP request headers as span attributes, set the environment variable
``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`` to a comma delimited list of HTTP header names.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST="content-type,custom_request_header"

will extract ``content-type`` and ``custom_request_header`` from the request headers and add them as span attributes.

Request header names in ASGI are case-insensitive. So, giving the header name as ``CUStom-Header`` in the environment
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
list containing the header values.

For example:
``http.request.header.custom_request_header = ["<value1>", "<value2>"]``

Response headers
****************
To capture HTTP response headers as span attributes, set the environment variable
``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE`` to a comma delimited list of HTTP header names.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE="content-type,custom_response_header"

will extract ``content-type`` and ``custom_response_header`` from the response headers and add them as span attributes.

Response header names in ASGI are case-insensitive. So, giving the header name as ``CUStom-Header`` in the environment
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
to a comma delimited list of HTTP header names to be sanitized.  Regexes may be used, and all header names will be
matched in a case-insensitive manner.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS=".*session.*,set-cookie"

will replace the value of headers such as ``session-id`` and ``set-cookie`` with ``[REDACTED]`` in the span.

Note:
    The environment variable names used to capture HTTP headers are still experimental, and thus are subject to change.

API
---
"""

from __future__ import annotations

import typing
import urllib
from collections import defaultdict
from functools import wraps
from timeit import default_timer
from typing import Any, Awaitable, Callable, DefaultDict, Tuple

from asgiref.compatibility import guarantee_single_callable

from opentelemetry import context, trace
from opentelemetry.instrumentation._semconv import (
    _filter_semconv_active_request_count_attr,
    _filter_semconv_duration_attrs,
    _get_schema_url,
    _HTTPStabilityMode,
    _OpenTelemetrySemanticConventionStability,
    _OpenTelemetryStabilitySignalType,
    _report_new,
    _report_old,
    _server_active_requests_count_attrs_new,
    _server_active_requests_count_attrs_old,
    _server_duration_attrs_new,
    _server_duration_attrs_old,
    _set_http_flavor_version,
    _set_http_host_server,
    _set_http_method,
    _set_http_net_host_port,
    _set_http_peer_ip_server,
    _set_http_peer_port_server,
    _set_http_scheme,
    _set_http_target,
    _set_http_url,
    _set_http_user_agent,
    _set_status,
)
from opentelemetry.instrumentation.asgi.types import (
    ClientRequestHook,
    ClientResponseHook,
    ServerRequestHook,
)
from opentelemetry.instrumentation.asgi.version import __version__  # noqa
from opentelemetry.instrumentation.propagators import (
    get_global_response_propagator,
)
from opentelemetry.instrumentation.utils import _start_internal_or_server_span
from opentelemetry.metrics import get_meter
from opentelemetry.propagators.textmap import Getter, Setter
from opentelemetry.semconv._incubating.metrics.http_metrics import (
    create_http_server_active_requests,
    create_http_server_request_body_size,
    create_http_server_response_body_size,
)
from opentelemetry.semconv.metrics import MetricInstruments
from opentelemetry.semconv.metrics.http_metrics import (
    HTTP_SERVER_REQUEST_DURATION,
)
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import set_span_in_context
from opentelemetry.util.http import (
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS,
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST,
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE,
    SanitizeValue,
    _parse_url_query,
    get_custom_headers,
    normalise_request_header_name,
    normalise_response_header_name,
    remove_url_credentials,
    sanitize_method,
)


class ASGIGetter(Getter[dict]):
    def get(
        self, carrier: dict, key: str
    ) -> typing.Optional[typing.List[str]]:
        """Getter implementation to retrieve a HTTP header value from the ASGI
        scope.

        Args:
            carrier: ASGI scope object
            key: header name in scope
        Returns:
            A list with a single string with the header value if it exists,
                else None.
        """
        headers = carrier.get("headers")
        if not headers:
            return None

        # ASGI header keys are in lower case
        key = key.lower()
        decoded = [
            _value.decode("utf8")
            for (_key, _value) in headers
            if _key.decode("utf8").lower() == key
        ]
        if not decoded:
            return None
        return decoded

    def keys(self, carrier: dict) -> typing.List[str]:
        headers = carrier.get("headers") or []
        return [_key.decode("utf8") for (_key, _value) in headers]


asgi_getter = ASGIGetter()


class ASGISetter(Setter[dict]):
    def set(
        self, carrier: dict, key: str, value: str
    ) -> None:  # pylint: disable=no-self-use
        """Sets response header values on an ASGI scope according to `the spec <https://asgi.readthedocs.io/en/latest/specs/www.html#response-start-send-event>`_.

        Args:
            carrier: ASGI scope object
            key: response header name to set
            value: response header value
        Returns:
            None
        """
        headers = carrier.get("headers")
        if not headers:
            headers = []
            carrier["headers"] = headers

        headers.append([key.lower().encode(), value.encode()])


asgi_setter = ASGISetter()


# pylint: disable=too-many-branches
def collect_request_attributes(
    scope, sem_conv_opt_in_mode=_HTTPStabilityMode.DEFAULT
):
    """Collects HTTP request attributes from the ASGI scope and returns a
    dictionary to be used as span creation attributes."""
    server_host, port, http_url = get_host_port_url_tuple(scope)
    query_string = scope.get("query_string")
    if query_string and http_url:
        if isinstance(query_string, bytes):
            query_string = query_string.decode("utf8")
        http_url += "?" + urllib.parse.unquote(query_string)
    result = {}

    scheme = scope.get("scheme")
    if scheme:
        _set_http_scheme(result, scheme, sem_conv_opt_in_mode)
    if server_host:
        _set_http_host_server(result, server_host, sem_conv_opt_in_mode)
    if port:
        _set_http_net_host_port(result, port, sem_conv_opt_in_mode)
    flavor = scope.get("http_version")
    if flavor:
        _set_http_flavor_version(result, flavor, sem_conv_opt_in_mode)
    path = scope.get("path")
    if path:
        _set_http_target(
            result, path, path, query_string, sem_conv_opt_in_mode
        )
    if http_url:
        if _report_old(sem_conv_opt_in_mode):
            _set_http_url(
                result,
                remove_url_credentials(http_url),
                _HTTPStabilityMode.DEFAULT,
            )
    http_method = scope.get("method", "")
    if http_method:
        _set_http_method(
            result,
            http_method,
            sanitize_method(http_method),
            sem_conv_opt_in_mode,
        )

    http_host_value_list = asgi_getter.get(scope, "host")
    if http_host_value_list:
        if _report_old(sem_conv_opt_in_mode):
            result[SpanAttributes.HTTP_SERVER_NAME] = ",".join(
                http_host_value_list
            )
    http_user_agent = asgi_getter.get(scope, "user-agent")
    if http_user_agent:
        _set_http_user_agent(result, http_user_agent[0], sem_conv_opt_in_mode)

    if "client" in scope and scope["client"] is not None:
        _set_http_peer_ip_server(
            result, scope.get("client")[0], sem_conv_opt_in_mode
        )
        _set_http_peer_port_server(
            result, scope.get("client")[1], sem_conv_opt_in_mode
        )

    # remove None values
    result = {k: v for k, v in result.items() if v is not None}

    return result


def collect_custom_headers_attributes(
    scope_or_response_message: dict[str, Any],
    sanitize: SanitizeValue,
    header_regexes: list[str],
    normalize_names: Callable[[str], str],
) -> dict[str, list[str]]:
    """
    Returns custom HTTP request or response headers to be added into SERVER span as span attributes.

    Refer specifications:
     - https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-request-and-response-headers
    """
    headers: DefaultDict[str, list[str]] = defaultdict(list)
    raw_headers = scope_or_response_message.get("headers")
    if raw_headers:
        for key, value in raw_headers:
            # Decode headers before processing.
            headers[key.decode()].append(value.decode())

    return sanitize.sanitize_header_values(
        headers,
        header_regexes,
        normalize_names,
    )


def get_host_port_url_tuple(scope):
    """Returns (host, port, full_url) tuple."""
    server = scope.get("server") or ["0.0.0.0", 80]
    port = server[1]
    server_host = server[0] + (":" + str(port) if str(port) != "80" else "")
    # using the scope path is enough, see:
    # - https://asgi.readthedocs.io/en/latest/specs/www.html#http-connection-scope (see: root_path and path)
    # - https://asgi.readthedocs.io/en/latest/specs/www.html#wsgi-compatibility (see: PATH_INFO)
    #       PATH_INFO can be derived by stripping root_path from path
    #       -> that means that the path should contain the root_path already, so prefixing it again is not necessary
    # - https://wsgi.readthedocs.io/en/latest/definitions.html#envvar-PATH_INFO
    full_path = scope.get("path", "")
    http_url = scope.get("scheme", "http") + "://" + server_host + full_path
    return server_host, port, http_url


def set_status_code(
    span,
    status_code,
    metric_attributes=None,
    sem_conv_opt_in_mode=_HTTPStabilityMode.DEFAULT,
):
    """Adds HTTP response attributes to span using the status_code argument."""
    status_code_str = str(status_code)

    try:
        status_code = int(status_code)
    except ValueError:
        status_code = -1
    if metric_attributes is None:
        metric_attributes = {}
    _set_status(
        span,
        metric_attributes,
        status_code,
        status_code_str,
        server_span=True,
        sem_conv_opt_in_mode=sem_conv_opt_in_mode,
    )


def get_default_span_details(scope: dict) -> Tuple[str, dict]:
    """
    Default span name is the HTTP method and URL path, or just the method.
    https://github.com/open-telemetry/opentelemetry-specification/pull/3165
    https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/http/#name

    Args:
        scope: the ASGI scope dictionary
    Returns:
        a tuple of the span name, and any attributes to attach to the span.
    """
    path = scope.get("path", "").strip()
    method = sanitize_method(scope.get("method", "").strip())
    if method == "_OTHER":
        method = "HTTP"
    if method and path:  # http
        return f"{method} {path}", {}
    if path:  # websocket
        return path, {}
    return method, {}  # http with no path


def _collect_target_attribute(
    scope: typing.Dict[str, typing.Any]
) -> typing.Optional[str]:
    """
    Returns the target path as defined by the Semantic Conventions.

    This value is suitable to use in metrics as it should replace concrete
    values with a parameterized name. Example: /api/users/{user_id}

    Refer to the specification
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/semantic_conventions/http-metrics.md#parameterized-attributes

    Note: this function requires specific code for each framework, as there's no
    standard attribute to use.
    """
    # FastAPI
    root_path = scope.get("root_path", "")

    route = scope.get("route")
    path_format = getattr(route, "path_format", None)
    if path_format:
        return f"{root_path}{path_format}"

    return None


class OpenTelemetryMiddleware:
    """The ASGI application middleware.

    This class is an ASGI middleware that starts and annotates spans for any
    requests it is invoked with.

    Args:
        app: The ASGI application callable to forward requests to.
        default_span_details: Callback which should return a string and a tuple, representing the desired default span name and a
                      dictionary with any additional span attributes to set.
                      Optional: Defaults to get_default_span_details.
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
    """

    # pylint: disable=too-many-branches
    def __init__(
        self,
        app,
        excluded_urls=None,
        default_span_details=None,
        server_request_hook: ServerRequestHook = None,
        client_request_hook: ClientRequestHook = None,
        client_response_hook: ClientResponseHook = None,
        tracer_provider=None,
        meter_provider=None,
        tracer=None,
        meter=None,
        http_capture_headers_server_request: list[str] | None = None,
        http_capture_headers_server_response: list[str] | None = None,
        http_capture_headers_sanitize_fields: list[str] | None = None,
    ):
        # initialize semantic conventions opt-in if needed
        _OpenTelemetrySemanticConventionStability._initialize()
        sem_conv_opt_in_mode = _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
            _OpenTelemetryStabilitySignalType.HTTP,
        )
        self.app = guarantee_single_callable(app)
        self.tracer = (
            trace.get_tracer(
                __name__,
                __version__,
                tracer_provider,
                schema_url=_get_schema_url(sem_conv_opt_in_mode),
            )
            if tracer is None
            else tracer
        )
        self.meter = (
            get_meter(
                __name__,
                __version__,
                meter_provider,
                schema_url=_get_schema_url(sem_conv_opt_in_mode),
            )
            if meter is None
            else meter
        )
        self.duration_histogram_old = None
        if _report_old(sem_conv_opt_in_mode):
            self.duration_histogram_old = self.meter.create_histogram(
                name=MetricInstruments.HTTP_SERVER_DURATION,
                unit="ms",
                description="Duration of HTTP server requests.",
            )
        self.duration_histogram_new = None
        if _report_new(sem_conv_opt_in_mode):
            self.duration_histogram_new = self.meter.create_histogram(
                name=HTTP_SERVER_REQUEST_DURATION,
                description="Duration of HTTP server requests.",
                unit="s",
            )
        self.server_response_size_histogram = None
        if _report_old(sem_conv_opt_in_mode):
            self.server_response_size_histogram = self.meter.create_histogram(
                name=MetricInstruments.HTTP_SERVER_RESPONSE_SIZE,
                unit="By",
                description="measures the size of HTTP response messages (compressed).",
            )
        self.server_response_body_size_histogram = None
        if _report_new(sem_conv_opt_in_mode):
            self.server_response_body_size_histogram = (
                create_http_server_response_body_size(self.meter)
            )
        self.server_request_size_histogram = None
        if _report_old(sem_conv_opt_in_mode):
            self.server_request_size_histogram = self.meter.create_histogram(
                name=MetricInstruments.HTTP_SERVER_REQUEST_SIZE,
                unit="By",
                description="Measures the size of HTTP request messages (compressed).",
            )
        self.server_request_body_size_histogram = None
        if _report_new(sem_conv_opt_in_mode):
            self.server_request_body_size_histogram = (
                create_http_server_request_body_size(self.meter)
            )
        self.active_requests_counter = create_http_server_active_requests(
            self.meter
        )
        self.excluded_urls = excluded_urls
        self.default_span_details = (
            default_span_details or get_default_span_details
        )
        self.server_request_hook = server_request_hook
        self.client_request_hook = client_request_hook
        self.client_response_hook = client_response_hook
        self.content_length_header = None
        self._sem_conv_opt_in_mode = sem_conv_opt_in_mode

        # Environment variables as constructor parameters
        self.http_capture_headers_server_request = (
            http_capture_headers_server_request
            or (
                get_custom_headers(
                    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST
                )
            )
            or None
        )
        self.http_capture_headers_server_response = (
            http_capture_headers_server_response
            or (
                get_custom_headers(
                    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE
                )
            )
            or None
        )
        self.http_capture_headers_sanitize_fields = SanitizeValue(
            http_capture_headers_sanitize_fields
            or (
                get_custom_headers(
                    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS
                )
            )
            or []
        )

    # pylint: disable=too-many-statements
    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        """The ASGI application

        Args:
            scope: An ASGI environment.
            receive: An awaitable callable yielding dictionaries
            send: An awaitable callable taking a single dictionary as argument.
        """
        start = default_timer()
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)

        _, _, url = get_host_port_url_tuple(scope)
        if self.excluded_urls and self.excluded_urls.url_disabled(url):
            return await self.app(scope, receive, send)

        span_name, additional_attributes = self.default_span_details(scope)

        attributes = collect_request_attributes(
            scope, self._sem_conv_opt_in_mode
        )
        attributes.update(additional_attributes)
        span, token = _start_internal_or_server_span(
            tracer=self.tracer,
            span_name=span_name,
            start_time=None,
            context_carrier=scope,
            context_getter=asgi_getter,
            attributes=attributes,
        )
        active_requests_count_attrs = _parse_active_request_count_attrs(
            attributes,
            self._sem_conv_opt_in_mode,
        )

        if scope["type"] == "http":
            self.active_requests_counter.add(1, active_requests_count_attrs)
        try:
            with trace.use_span(span, end_on_exit=False) as current_span:
                if current_span.is_recording():
                    for key, value in attributes.items():
                        current_span.set_attribute(key, value)

                    if current_span.kind == trace.SpanKind.SERVER:
                        custom_attributes = (
                            collect_custom_headers_attributes(
                                scope,
                                self.http_capture_headers_sanitize_fields,
                                self.http_capture_headers_server_request,
                                normalise_request_header_name,
                            )
                            if self.http_capture_headers_server_request
                            else {}
                        )
                        if len(custom_attributes) > 0:
                            current_span.set_attributes(custom_attributes)

                if callable(self.server_request_hook):
                    self.server_request_hook(current_span, scope)

                otel_receive = self._get_otel_receive(
                    span_name, scope, receive
                )

                otel_send = self._get_otel_send(
                    current_span,
                    span_name,
                    scope,
                    send,
                    attributes,
                )

                await self.app(scope, otel_receive, otel_send)
        finally:
            if scope["type"] == "http":
                target = _collect_target_attribute(scope)
                if target:
                    path, query = _parse_url_query(target)
                    _set_http_target(
                        attributes,
                        target,
                        path,
                        query,
                        self._sem_conv_opt_in_mode,
                    )
                duration_s = default_timer() - start
                duration_attrs_old = _parse_duration_attrs(
                    attributes, _HTTPStabilityMode.DEFAULT
                )
                if target:
                    duration_attrs_old[SpanAttributes.HTTP_TARGET] = target
                duration_attrs_new = _parse_duration_attrs(
                    attributes, _HTTPStabilityMode.HTTP
                )
                if self.duration_histogram_old:
                    self.duration_histogram_old.record(
                        max(round(duration_s * 1000), 0), duration_attrs_old
                    )
                if self.duration_histogram_new:
                    self.duration_histogram_new.record(
                        max(duration_s, 0), duration_attrs_new
                    )
                self.active_requests_counter.add(
                    -1, active_requests_count_attrs
                )
                if self.content_length_header:
                    if self.server_response_size_histogram:
                        self.server_response_size_histogram.record(
                            self.content_length_header, duration_attrs_old
                        )
                    if self.server_response_body_size_histogram:
                        self.server_response_body_size_histogram.record(
                            self.content_length_header, duration_attrs_new
                        )

                request_size = asgi_getter.get(scope, "content-length")
                if request_size:
                    try:
                        request_size_amount = int(request_size[0])
                    except ValueError:
                        pass
                    else:
                        if self.server_request_size_histogram:
                            self.server_request_size_histogram.record(
                                request_size_amount, duration_attrs_old
                            )
                        if self.server_request_body_size_histogram:
                            self.server_request_body_size_histogram.record(
                                request_size_amount, duration_attrs_new
                            )
            if token:
                context.detach(token)
            if span.is_recording():
                span.end()

    # pylint: enable=too-many-branches

    def _get_otel_receive(self, server_span_name, scope, receive):
        @wraps(receive)
        async def otel_receive():
            with self.tracer.start_as_current_span(
                " ".join((server_span_name, scope["type"], "receive"))
            ) as receive_span:
                message = await receive()
                if callable(self.client_request_hook):
                    self.client_request_hook(receive_span, scope, message)
                if receive_span.is_recording():
                    if message["type"] == "websocket.receive":
                        set_status_code(
                            receive_span,
                            200,
                            None,
                            self._sem_conv_opt_in_mode,
                        )
                    receive_span.set_attribute(
                        "asgi.event.type", message["type"]
                    )
            return message

        return otel_receive

    def _get_otel_send(
        self,
        server_span,
        server_span_name,
        scope,
        send,
        duration_attrs,
    ):
        expecting_trailers = False

        @wraps(send)
        async def otel_send(message: dict[str, Any]):
            nonlocal expecting_trailers
            with self.tracer.start_as_current_span(
                " ".join((server_span_name, scope["type"], "send"))
            ) as send_span:
                if callable(self.client_response_hook):
                    self.client_response_hook(send_span, scope, message)

                status_code = None
                if message["type"] == "http.response.start":
                    status_code = message["status"]
                elif message["type"] == "websocket.send":
                    status_code = 200

                if send_span.is_recording():
                    if message["type"] == "http.response.start":
                        expecting_trailers = message.get("trailers", False)
                    send_span.set_attribute("asgi.event.type", message["type"])
                    if (
                        server_span.is_recording()
                        and server_span.kind == trace.SpanKind.SERVER
                        and "headers" in message
                    ):
                        custom_response_attributes = (
                            collect_custom_headers_attributes(
                                message,
                                self.http_capture_headers_sanitize_fields,
                                self.http_capture_headers_server_response,
                                normalise_response_header_name,
                            )
                            if self.http_capture_headers_server_response
                            else {}
                        )
                        if len(custom_response_attributes) > 0:
                            server_span.set_attributes(
                                custom_response_attributes
                            )
                if status_code:
                    # We record metrics only once
                    set_status_code(
                        server_span,
                        status_code,
                        duration_attrs,
                        self._sem_conv_opt_in_mode,
                    )
                    set_status_code(
                        send_span,
                        status_code,
                        None,
                        self._sem_conv_opt_in_mode,
                    )

                propagator = get_global_response_propagator()
                if propagator:
                    propagator.inject(
                        message,
                        context=set_span_in_context(
                            server_span, trace.context_api.Context()
                        ),
                        setter=asgi_setter,
                    )

                content_length = asgi_getter.get(message, "content-length")
                if content_length:
                    try:
                        self.content_length_header = int(content_length[0])
                    except ValueError:
                        pass

                await send(message)
            # pylint: disable=too-many-boolean-expressions
            if (
                not expecting_trailers
                and message["type"] == "http.response.body"
                and not message.get("more_body", False)
            ) or (
                expecting_trailers
                and message["type"] == "http.response.trailers"
                and not message.get("more_trailers", False)
            ):
                server_span.end()

        return otel_send


def _parse_duration_attrs(
    req_attrs, sem_conv_opt_in_mode=_HTTPStabilityMode.DEFAULT
):
    return _filter_semconv_duration_attrs(
        req_attrs,
        _server_duration_attrs_old,
        _server_duration_attrs_new,
        sem_conv_opt_in_mode,
    )


def _parse_active_request_count_attrs(
    req_attrs, sem_conv_opt_in_mode=_HTTPStabilityMode.DEFAULT
):
    return _filter_semconv_active_request_count_attr(
        req_attrs,
        _server_active_requests_count_attrs_old,
        _server_active_requests_count_attrs_new,
        sem_conv_opt_in_mode,
    )
