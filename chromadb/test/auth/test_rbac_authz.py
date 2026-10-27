import uuid

import httpx
import pytest

from chromadb.api import ServerAPI
from chromadb.config import DEFAULT_TENANT


def test_delete_database_requires_rbac_permission(
    api_with_authn_rbac_authz: ServerAPI,
) -> None:
    database_name = f"db_{uuid.uuid4().hex}"
    api_with_authn_rbac_authz.create_database(database_name, tenant=DEFAULT_TENANT)

    with pytest.raises(Exception, match="Forbidden"):
        api_with_authn_rbac_authz.delete_database(database_name, tenant=DEFAULT_TENANT)

    db = api_with_authn_rbac_authz.get_database(database_name, tenant=DEFAULT_TENANT)
    assert db["name"] == database_name


def test_pre_flight_checks_requires_auth(
    api_with_authn_rbac_authz: ServerAPI,
) -> None:
    response = httpx.get(f"{api_with_authn_rbac_authz.get_api_url()}/pre-flight-checks")

    assert response.status_code == 403
