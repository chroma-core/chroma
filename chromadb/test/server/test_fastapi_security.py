from typing import Any, Callable, Dict, List

import orjson
import pytest
from anyio import to_thread

import chromadb.server.fastapi as fastapi_server
from chromadb.server.fastapi import FastAPI


class FakeRequest:
    headers: Dict[str, str] = {}

    def __init__(self, body: Dict[str, Any]) -> None:
        self._body = orjson.dumps(body)

    async def body(self) -> bytes:
        return self._body


class ExplodingApi:
    def create_collection(self, **_kwargs: Any) -> None:
        raise AssertionError("collection creation should not be reached")


class NoopRateLimitEnforcer:
    def rate_limit(self, func: Callable[..., Any]) -> Callable[..., Any]:
        return func


async def run_sync_immediately(
    func: Callable[..., Any], *args: Any, limiter: Any = None
) -> Any:
    del limiter
    return func(*args)


def create_collection_body() -> Dict[str, Any]:
    return {
        "name": "poisoned",
        "configuration": {
            "embedding_function": {
                "name": "sentence_transformer",
                "type": "known",
                "config": {
                    "model_name": "attacker/model",
                    "device": "cpu",
                    "normalize_embeddings": False,
                    "kwargs": {"model_kwargs": {"trust_remote_code": True}},
                },
            }
        },
    }


def make_uninitialized_fastapi() -> Any:
    server: Any = FastAPI.__new__(FastAPI)
    server._api = ExplodingApi()
    server._capacity_limiter = None
    server._async_rate_limit_enforcer = NoopRateLimitEnforcer()
    server._set_request_context = lambda request: None
    return server


@pytest.mark.asyncio
async def test_v2_create_collection_authenticates_before_loading_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: List[str] = []
    server = make_uninitialized_fastapi()

    def fail_auth(*_args: Any, **_kwargs: Any) -> None:
        calls.append("auth")
        raise RuntimeError("unauthorized")

    def load_configuration(config: Dict[str, Any]) -> Dict[str, Any]:
        del config
        calls.append("load_configuration")
        return {}

    server.sync_auth_request = fail_auth
    monkeypatch.setattr(to_thread, "run_sync", run_sync_immediately)
    monkeypatch.setattr(
        fastapi_server,
        "load_create_collection_configuration_from_json",
        load_configuration,
    )

    with pytest.raises(RuntimeError, match="unauthorized"):
        await server.create_collection(
            FakeRequest(create_collection_body()),
            tenant="default_tenant",
            database_name="default_database",
        )

    assert calls == ["auth"]


@pytest.mark.asyncio
async def test_v1_create_collection_authenticates_before_loading_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: List[str] = []
    server = make_uninitialized_fastapi()

    def fail_auth(*_args: Any, **_kwargs: Any) -> None:
        calls.append("auth")
        raise RuntimeError("unauthorized")

    def load_configuration(config: Dict[str, Any]) -> Dict[str, Any]:
        del config
        calls.append("load_configuration")
        return {}

    server.sync_auth_and_get_tenant_and_database_for_request = fail_auth
    monkeypatch.setattr(to_thread, "run_sync", run_sync_immediately)
    monkeypatch.setattr(
        fastapi_server,
        "load_create_collection_configuration_from_json",
        load_configuration,
    )

    with pytest.raises(RuntimeError, match="unauthorized"):
        await server.create_collection_v1(
            FakeRequest(create_collection_body()),
            tenant="default_tenant",
            database="default_database",
        )

    assert calls == ["auth"]
