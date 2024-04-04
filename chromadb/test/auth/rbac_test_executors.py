from typing import Any
import uuid
from hypothesis import given
from hypothesis.strategies import data
from typing import Callable, Dict

from chromadb.api import AdminAPI, ServerAPI

# Each of these accepts four clients:
# 1. The data plane client with credentials of the user under test.
# 2. The admin client with credentials of the user under test.
# 3. The data plane client with credentials of the root user.
# 4. The admin client with credentials of the root user.
# Not every executor uses all four clients, but it's easier to accept them all
# than to have a separate signature for each executor.
#
# We need the root user clients to ensure preconditions are met: if we want to
# test e.g. get_tenant, we have to make sure the tenant exists first.


@given(data())
def _create_tenant_executor(
    _api: ServerAPI,
    admin_api: AdminAPI,
    _root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    tenant = str(data.draw(uuid.uuid4()))
    admin_api.create_tenant(tenant)


@given(data())
def _get_tenant_executor(
    _api: ServerAPI,
    admin_api: AdminAPI,
    _root_api: ServerAPI,
    root_admin_api: AdminAPI,
    data: Any
) -> None:
    tenant = str(data.draw(uuid.uuid4()))
    root_admin_api.create_tenant(tenant)
    admin_api.get_tenant(tenant)


@given(data())
def _create_database_executor(
    _api: ServerAPI,
    admin_api: AdminAPI,
    _root_api: ServerAPI,
    root_admin_api: AdminAPI,
    data: Any
) -> None:
    tenant = str(data.draw(uuid.uuid4()))
    root_admin_api.create_tenant(tenant)
    database = str(data.draw(uuid.uuid4()))
    admin_api.create_database(tenant, database)


@given(data())
def _get_database_executor(
    _api: ServerAPI,
    admin_api: AdminAPI,
    _root_api: ServerAPI,
    root_admin_api: AdminAPI,
    data: Any
) -> None:
    tenant = str(data.draw(uuid.uuid4()))
    root_admin_api.create_tenant(tenant)
    database = str(data.draw(uuid.uuid4()))
    root_admin_api.create_database(tenant, database)
    admin_api.get_database(tenant, database)


def _reset_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    _root_api: ServerAPI,
    _root_admin_api: AdminAPI,
) -> None:
    api.reset()


@given(data())
def _list_collections_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    _root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    api.list_collections()


@given(data())
def _get_collection_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_api.create_collection(collection)
    api.get_collection(collection)


@given(data())
def _create_collection_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    _root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    api.create_collection(collection)


@given(data())
def _get_or_create_collection_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    _root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    api.get_or_create_collection(collection)


@given(data())
def _delete_collection_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    _root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    api.delete_collection(collection)


@given(data())
def _update_collection_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_api.create_collection(collection)
    col = api.get_collection(collection)
    col.modify(metadata={"foo": "bar"})


@given(data())
def _add_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_api.create_collection(collection)
    col = api.get_collection(collection)
    col.add(ids=["1"], documents=["test document"])


@given(data())
def _delete_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_col = root_api.create_collection(collection)
    root_col.add(ids=["1"], documents=["test document"])
    col = api.get_collection(collection)
    col.delete(ids=["1"])


@given(data())
def _get_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_col = root_api.create_collection(collection)
    root_col.add(ids=["1"], documents=["test document"])
    col = api.get_collection(collection)
    col.get(ids=["1"])


@given(data())
def _query_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_col = root_api.create_collection(collection)
    root_col.add(ids=["1"], documents=["test document"])
    col = api.get_collection(collection)
    col.query(query_texts=["test query text"])


@given(data())
def _peek_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_col = root_api.create_collection(collection)
    root_col.add(ids=["1"], documents=["test document"])
    col = api.get_collection(collection)
    col.peek()


@given(data())
def _count_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_col = root_api.create_collection(collection)
    root_col.add(ids=["1"], documents=["test document"])
    col = api.get_collection(collection)
    col.count()


@given(data())
def _update_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_col = root_api.create_collection(collection)
    root_col.add(ids=["1"], documents=["test document"])
    col = api.get_collection(collection)
    col.update(ids=["1"], documents=["different test document"])


@given(data())
def _upsert_executor(
    api: ServerAPI,
    _admin_api: AdminAPI,
    root_api: ServerAPI,
    _root_admin_api: AdminAPI,
    data: Any
) -> None:
    collection = str(data.draw(uuid.uuid4()))
    root_col = root_api.create_collection(collection)
    root_col.add(ids=["1"], documents=["test document"])
    col = api.get_collection(collection)
    col.upsert(ids=["1"], documents=["different test document"])


api_executors: Dict[
        str,
        Callable[[ServerAPI, AdminAPI, ServerAPI, AdminAPI], None]] = {
    "tenant:create_tenant": _create_tenant_executor,
    "tenant:get_tenant": _get_tenant_executor,
    "db:create_database": _create_database_executor,
    "db:get_database": _get_database_executor,
    "db:reset": _reset_executor,
    "db:list_collections": _list_collections_executor,
    "collection:get_collection": _get_collection_executor,
    "collection:create_collection": _create_collection_executor,
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
