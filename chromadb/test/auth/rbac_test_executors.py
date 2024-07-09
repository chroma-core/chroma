import hypothesis.strategies as st
from typing import Callable, Dict

from chromadb.api import ServerAPI
from chromadb.config import DEFAULT_TENANT, DEFAULT_DATABASE
from chromadb.test.property.strategies import (
    collection_name,
    tenant_database_name,
)
from chromadb.api.models.Collection import Collection
from chromadb.types import Collection as CollectionModel


def wrap_model(api: ServerAPI, model: CollectionModel) -> Collection:
    return Collection(client=api, model=model)


def add_to_root_and_get_collection(
    api: ServerAPI, root_api: ServerAPI, draw: st.DrawFn
) -> Collection:
    collection = draw(collection_name())
    root_col = None
    try:
        root_col = root_api.create_collection(collection)
    except Exception:
        root_col = root_api.get_collection(collection)
    if not root_col:
        raise Exception("Failed to create collection")
    root_col = wrap_model(api=root_api, model=root_col)
    root_col.add(ids=["1"], documents=["test document"])
    col = wrap_model(api=api, model=api.get_collection(collection))
    return col


# Each of these accepts two clients:
# 1. A data plane client with credentials of the user under test.
# 2. A data plane client with credentials of the root user.
# Not every executor uses bothclients, but it's easier to accept them all
# than to have a separate signature for each executor.
#
# We need the root user clients to ensure preconditions are met: if we want to
# test e.g. get_tenant, we have to make sure the tenant exists first.


def _create_tenant_executor(
    api: ServerAPI, _root_api: ServerAPI, draw: st.DrawFn
) -> None:
    tenant = draw(tenant_database_name)
    try:
        api.create_tenant(tenant)
    except Exception:
        pass


def _get_tenant_executor(
    api: ServerAPI,
    _root_api: ServerAPI,
    _draw: st.DrawFn,
) -> None:
    api.get_tenant(DEFAULT_TENANT)


def _create_database_executor(
    api: ServerAPI, _root_api: ServerAPI, draw: st.DrawFn
) -> None:
    database = draw(tenant_database_name)
    try:
        api.create_database(database, DEFAULT_TENANT)
    except Exception:
        pass


def _get_database_executor(
    api: ServerAPI,
    _root_api: ServerAPI,
    _draw: st.DrawFn,
) -> None:
    api.get_database(DEFAULT_DATABASE, DEFAULT_TENANT)


def _reset_executor(
    api: ServerAPI,
    _root_api: ServerAPI,
    _draw: st.DrawFn,
) -> None:
    api.reset()


def _list_collections_executor(
    api: ServerAPI,
    _root_api: ServerAPI,
    _draw: st.DrawFn,
) -> None:
    api.list_collections()


def _get_collection_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    collection = draw(collection_name())
    try:
        root_api.create_collection(collection)
    except Exception:
        pass
    api.get_collection(collection)


def _create_collection_executor(
    api: ServerAPI,
    _root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    collection = draw(collection_name())
    api.create_collection(collection)


def _get_or_create_collection_executor(
    api: ServerAPI,
    _root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    collection = draw(collection_name())
    try:
        api.get_or_create_collection(collection)
    except Exception:
        pass


def _delete_collection_executor(
    api: ServerAPI, root_api: ServerAPI, draw: st.DrawFn
) -> None:
    collection = draw(collection_name())
    try:
        root_api.create_collection(collection)
    except Exception:
        pass
    api.delete_collection(collection)


def _update_collection_executor(
    api: ServerAPI, root_api: ServerAPI, draw: st.DrawFn
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.modify(metadata={"foo": "bar"})


def _add_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.add(ids=["1"], documents=["test document"])


def _delete_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.delete(ids=["1"])


def _get_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.get(ids=["1"])


def _query_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.query(query_texts=["test query text"])


def _peek_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.peek()


def _count_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.count()


def _update_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.update(ids=["1"], documents=["different test document"])


def _upsert_executor(
    api: ServerAPI,
    root_api: ServerAPI,
    draw: st.DrawFn,
) -> None:
    col = add_to_root_and_get_collection(api, root_api, draw)
    col.upsert(ids=["1"], documents=["different test document"])


api_executors: Dict[str, Callable[[ServerAPI, ServerAPI, st.DrawFn], None]] = {
    "system:reset": _reset_executor,
    "tenant:create_tenant": _create_tenant_executor,
    "tenant:get_tenant": _get_tenant_executor,
    "db:create_database": _create_database_executor,
    "db:get_database": _get_database_executor,
    "db:list_collections": _list_collections_executor,
    "collection:get_collection": _get_collection_executor,
    "db:create_collection": _create_collection_executor,
    "db:get_or_create_collection": _get_or_create_collection_executor,
    "collection:delete_collection": _delete_collection_executor,
    "collection:update_collection": _update_collection_executor,
    "collection:add": _add_executor,
    "collection:delete": _delete_executor,
    "collection:get": _get_executor,
    "collection:query": _query_executor,
    "collection:peek": _peek_executor,
    "collection:count": _count_executor,
    "collection:update": _update_executor,
    "collection:upsert": _upsert_executor,
}
