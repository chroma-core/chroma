import pytest
from fastapi import HTTPException
from chromadb.auth import AuthzAction, AuthzResource, UserIdentity
from chromadb.auth.simple_rbac_authz import SimpleRBACAuthorizationProvider
from chromadb.config import Settings, System

def test_simple_rbac_tenant_isolation(tmp_path):
    config_file = tmp_path / "authz.yaml"
    config_file.write_text("""
users:
  - id: alice
    role: reader
roles_mapping:
  reader:
    actions:
      - "collection:get"
""")

    system = System(Settings(chroma_server_authz_config_file=str(config_file)))
    provider = SimpleRBACAuthorizationProvider(system)

    user_tenant_a = UserIdentity(user_id="alice", tenant="tenant_a")
    resource_tenant_a = AuthzResource(tenant="tenant_a", database="db_a", collection="col_1")
    
    # 1. Same tenant -> Authorized
    provider.authorize_or_raise(user_tenant_a, AuthzAction.GET, resource_tenant_a)

    # 2. Cross-tenant -> Raises 403 Forbidden
    resource_tenant_b = AuthzResource(tenant="tenant_b", database="db_a", collection="col_1")
    
    with pytest.raises(HTTPException) as exc_info:
        provider.authorize_or_raise(user_tenant_a, AuthzAction.GET, resource_tenant_b)
    
    assert exc_info.value.status_code == 403
