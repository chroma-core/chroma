#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import random
import time

import httpx


DEFAULT_CLOUD_HOST = "api.trychroma.com"
DEFAULT_CLOUD_PORT = 443
DEFAULT_CONNECT_TIMEOUT_SECS = 10.0
DEFAULT_READ_TIMEOUT_SECS = 300.0
DEFAULT_MAX_ATTEMPTS = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count vectors in a Chroma Cloud collection."
    )
    parser.add_argument("--api-key", default=os.environ.get("CHROMA_API_KEY"))
    parser.add_argument("--tenant", default=os.environ.get("CHROMA_TENANT"))
    parser.add_argument("--database", default=os.environ.get("CHROMA_DATABASE"))
    parser.add_argument("--collection", required=True)
    parser.add_argument("--cloud-host", default=DEFAULT_CLOUD_HOST)
    parser.add_argument("--cloud-port", type=int, default=DEFAULT_CLOUD_PORT)
    parser.add_argument("--disable-ssl", action="store_true")
    parser.add_argument("--read-level", default="index_and_wal")
    parser.add_argument(
        "--connect-timeout-secs", type=float, default=DEFAULT_CONNECT_TIMEOUT_SECS
    )
    parser.add_argument(
        "--read-timeout-secs", type=float, default=DEFAULT_READ_TIMEOUT_SECS
    )
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS)
    return parser.parse_args()


def require(value: str | None, name: str) -> str:
    if value:
        return value
    raise ValueError(f"Missing required argument: {name}")


def is_transient(exc: BaseException) -> bool:
    if isinstance(
        exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)
    ):
        return True
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    return status_code in {408, 429, 500, 502, 503, 504}


def backoff_sleep(attempt: int) -> None:
    delay = min(60.0, 2.0 * (2**attempt))
    delay += random.uniform(0.0, min(1.0, delay / 4.0))
    time.sleep(delay)


def request_with_retries(
    client: httpx.Client,
    method: str,
    url: str,
    max_attempts: int,
    *,
    params: dict[str, str] | None = None,
) -> httpx.Response:
    for attempt in range(max_attempts):
        try:
            response = client.request(method, url, params=params)
            response.raise_for_status()
            return response
        except BaseException as exc:
            if not is_transient(exc) or attempt == max_attempts - 1:
                raise
            backoff_sleep(attempt)
    raise RuntimeError("Unreachable")


def main() -> None:
    args = parse_args()
    api_key = require(args.api_key, "api_key / CHROMA_API_KEY")
    tenant = require(args.tenant, "tenant / CHROMA_TENANT")
    database = require(args.database, "database / CHROMA_DATABASE")

    scheme = "http" if args.disable_ssl else "https"
    base_url = f"{scheme}://{args.cloud_host}:{args.cloud_port}/api/v2"

    with httpx.Client(
        headers={
            "Content-Type": "application/json",
            "X-Chroma-Token": api_key,
            "User-Agent": "count-collection-vectors/1.0",
        },
        timeout=httpx.Timeout(
            connect=args.connect_timeout_secs,
            read=args.read_timeout_secs,
            write=args.connect_timeout_secs,
            pool=args.connect_timeout_secs,
        ),
    ) as client:
        collection_resp = request_with_retries(
            client,
            "GET",
            f"{base_url}/tenants/{tenant}/databases/{database}/collections/{args.collection}",
            args.max_attempts,
        )
        collection_id = collection_resp.json()["id"]

        count_resp = request_with_retries(
            client,
            "GET",
            f"{base_url}/tenants/{tenant}/databases/{database}/collections/{collection_id}/count",
            args.max_attempts,
            params={"read_level": args.read_level},
        )
        print(count_resp.json())


if __name__ == "__main__":
    main()
