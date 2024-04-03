import logging
from typing import Dict, Set
from overrides import override
import yaml
from chromadb.auth import (
    AuthzAction,
    UserIdentity,
    ServerAuthorizationProvider,
)
from chromadb.config import System

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
                "No configuration file (`chroma_server_authz_config_file`) "
                "provided for SimpleRBACAuthorizationProvider"
            )
        config_file = str(system.settings.chroma_server_authz_config_file)
        with open(config_file, "r") as f:
            self._config = yaml.safe_load(f)

        # We favor preprocessing here to avoid having to parse the config file
        # on every request. This AuthorizationProvider does not support
        # per-resource authorization so we just map the user ID to the
        # permissions they have. We're not worried about the size of this dict
        # since users are all specified in the file -- anyone with a gigantic
        # number of users can roll their own AuthorizationProvider.
        self._permissions: Dict[str, Set[str]] = {}
        for user in self._config["users"]:
            _actions = self._config["roles_mapping"][user["role"]]["actions"]
            self._permissions[user["user_id"]] = set(_actions)
        logger.info(
            "Authorization Provider SimpleRBACAuthorizationProvider "
            "initialized"
        )

    @trace_method(
        "SimpleRBACAuthorizationProvider.authorize",
        OpenTelemetryGranularity.ALL,
    )
    @override
    def authorize(self,
                  user: UserIdentity,
                  action: AuthzAction,
                  resource: str) -> bool:

        policy_decision = False
        if (user.user_id in self._permissions and
                action in self._permissions[user.user_id]):
            policy_decision = True
        logger.debug(
            f"Authorization decision: Access "
            f"{'granted' if policy_decision else 'denied'} for "
            f"user [{user.user_id}] attempting to "
            f"[{action}] [{resource}]"
        )
        return policy_decision
