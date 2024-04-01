import logging
from typing import Any, Dict, Set
from overrides import override
import yaml
from chromadb.auth import (
    AuthorizationContext,
    ServerAuthorizationProvider,
)
from chromadb.config import DEFAULT_TENANT, System

from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)

logger = logging.getLogger(__name__)


class SimpleRBACAuthorizationProvider(ServerAuthorizationProvider):
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings
        if not self._settings.chroma_server_authz_config_file:
            raise ValueError(
                "No configuration (CHROMA_SERVER_AUTHZ_CONFIG_FILE) file "
                "provided for SimpleRBACAuthorizationProvider"
            )
        config_file = str(system.settings.chroma_server_authz_config_file)
        with open(config_file, "r") as f:
            self._config = yaml.safe_load(f)

        self._authz_tuples_map: Dict[str, Set[Any]] = {}
        for u in self._config["users"]:
            _actions = self._config["roles_mapping"][u["role"]]["actions"]
            for a in _actions:
                tenant = u["tenant"] if "tenant" in u else DEFAULT_TENANT
                if u["id"] not in self._authz_tuples_map.keys():
                    self._authz_tuples_map[u["id"]] = set()
                self._authz_tuples_map[u["id"]].add(
                    (u["id"], tenant, *a.split(":"))
                )
        logger.debug(
            f"Loaded {len(self._authz_tuples_map)} permissions for "
            f"({len(self._config['users'])}) users"
        )
        logger.info(
            "Authorization Provider SimpleRBACAuthorizationProvider "
            "initialized"
        )

    @trace_method(
        "SimpleRBACAuthorizationProvider.authorize",
        OpenTelemetryGranularity.ALL,
    )
    @override
    def authorize(self, context: AuthorizationContext) -> bool:
        _authz_tuple = (
            context.user.id,
            context.user.tenant,
            context.resource.type,
            context.action.id,
        )

        policy_decision = False
        if (
            context.user.id in self._authz_tuples_map.keys()
            and _authz_tuple in self._authz_tuples_map[context.user.id]
        ):
            policy_decision = True
        logger.debug(
            f"Authorization decision: Access "
            f"{'granted' if policy_decision else 'denied'} for "
            f"user [{context.user.id}] attempting to [{context.action.id}]"
            f" on [{context.resource}]"
        )
        return policy_decision
