# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example of invisibly propagating a request ID with middleware."""

import argparse
import sys
import threading
import uuid

import pyarrow as pa
import pyarrow.flight as flight


class TraceContext:
    _locals = threading.local()
    _locals.trace_id = None

    @classmethod
    def current_trace_id(cls):
        if not getattr(cls._locals, "trace_id", None):
            cls.set_trace_id(uuid.uuid4().hex)
        return cls._locals.trace_id

    @classmethod
    def set_trace_id(cls, trace_id):
        cls._locals.trace_id = trace_id


TRACE_HEADER = "x-tracing-id"


class TracingServerMiddleware(flight.ServerMiddleware):
    def __init__(self, trace_id):
        self.trace_id = trace_id

    def sending_headers(self):
        return {
            TRACE_HEADER: self.trace_id,
        }


class TracingServerMiddlewareFactory(flight.ServerMiddlewareFactory):
    def start_call(self, info, headers):
        print("Starting new call:", info)
        if TRACE_HEADER in headers:
            trace_id = headers[TRACE_HEADER][0]
            print("Found trace header with value:", trace_id)
            TraceContext.set_trace_id(trace_id)
        return TracingServerMiddleware(TraceContext.current_trace_id())


class TracingClientMiddleware(flight.ClientMiddleware):
    def sending_headers(self):
        print("Sending trace ID:", TraceContext.current_trace_id())
        return {
            "x-tracing-id": TraceContext.current_trace_id(),
        }

    def received_headers(self, headers):
        if TRACE_HEADER in headers:
            trace_id = headers[TRACE_HEADER][0]
            print("Found trace header with value:", trace_id)
            # Don't overwrite our trace ID


class TracingClientMiddlewareFactory(flight.ClientMiddlewareFactory):
    def start_call(self, info):
        print("Starting new call:", info)
        return TracingClientMiddleware()


class FlightServer(flight.FlightServerBase):
    def __init__(self, delegate, **kwargs):
        super().__init__(**kwargs)
        if delegate:
            self.delegate = flight.connect(
                delegate,
                middleware=(TracingClientMiddlewareFactory(),))
        else:
            self.delegate = None

    def list_actions(self, context):
        return [
            ("get-trace-id", "Get the trace context ID."),
        ]

    def do_action(self, context, action):
        trace_middleware = context.get_middleware("trace")
        if trace_middleware:
            TraceContext.set_trace_id(trace_middleware.trace_id)
        if action.type == "get-trace-id":
            if self.delegate:
                for result in self.delegate.do_action(action):
                    yield result
            else:
                trace_id = TraceContext.current_trace_id().encode("utf-8")
                print("Returning trace ID:", trace_id)
                buf = pa.py_buffer(trace_id)
                yield pa.flight.Result(buf)
        else:
            raise KeyError(f"Unknown action {action.type!r}")


def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="command")
    client = subparsers.add_parser("client", help="Run the client.")
    client.add_argument("server")
    client.add_argument("--request-id", default=None)

    server = subparsers.add_parser("server", help="Run the server.")
    server.add_argument(
        "--listen",
        required=True,
        help="The location to listen on (example: grpc://localhost:5050)",
    )
    server.add_argument(
        "--delegate",
        required=False,
        default=None,
        help=("A location to delegate to. That is, this server will "
              "simply call the given server for the response. Demonstrates "
              "propagation of the trace ID between servers."),
    )

    args = parser.parse_args()
    if not getattr(args, "command"):
        parser.print_help()
        return 1

    if args.command == "server":
        server = FlightServer(
            args.delegate,
            location=args.listen,
            middleware={"trace": TracingServerMiddlewareFactory()})
        server.serve()
    elif args.command == "client":
        client = flight.connect(
            args.server,
            middleware=(TracingClientMiddlewareFactory(),))
        if args.request_id:
            TraceContext.set_trace_id(args.request_id)
        else:
            TraceContext.set_trace_id("client-chosen-id")

        for result in client.do_action(flight.Action("get-trace-id", b"")):
            print(result.body.to_pybytes())


if __name__ == "__main__":
    sys.exit(main() or 0)
