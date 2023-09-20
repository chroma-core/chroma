# A thin wrapper around the pulsar admin api
import requests
from chromadb.config import System
from chromadb.ingest.impl.utils import parse_topic_name


class PulsarAdmin:
    """A thin wrapper around the pulsar admin api, only used for interim development towards distributed chroma.
    This functionality will be moved to the chroma coordinator."""

    _connection_str: str

    def __init__(self, system: System):
        pulsar_host = system.settings.require("pulsar_broker_url")
        pulsar_port = system.settings.require("pulsar_admin_port")
        self._connection_str = f"http://{pulsar_host}:{pulsar_port}"

        # Create the default tenant and namespace
        # This is a temporary workaround until we have a proper tenant/namespace management system
        self.create_tenant("default")
        self.create_namespace("default", "default")

    def create_tenant(self, tenant: str) -> None:
        """Make a PUT request to the admin api to create the tenant"""

        path = f"/admin/v2/tenants/{tenant}"
        url = self._connection_str + path
        response = requests.put(
            url, json={"allowedClusters": ["standalone"], "adminRoles": []}
        )  # TODO: how to manage clusters?

        if response.status_code != 204 and response.status_code != 409:
            raise RuntimeError(f"Failed to create tenant {tenant}")

    def create_namespace(self, tenant: str, namespace: str) -> None:
        """Make a PUT request to the admin api to create the namespace"""

        path = f"/admin/v2/namespaces/{tenant}/{namespace}"
        url = self._connection_str + path
        response = requests.put(url)

        if response.status_code != 204 and response.status_code != 409:
            raise RuntimeError(f"Failed to create namespace {namespace}")

    def create_topic(self, topic: str) -> None:
        # TODO: support non-persistent topics?
        tenant, namespace, topic_name = parse_topic_name(topic)

        if tenant != "default":
            raise ValueError(f"Only the default tenant is supported, got {tenant}")
        if namespace != "default":
            raise ValueError(
                f"Only the default namespace is supported, got {namespace}"
            )

        # Make a PUT request to the admin api to create the topic
        path = f"/admin/v2/persistent/{tenant}/{namespace}/{topic_name}"
        url = self._connection_str + path
        response = requests.put(url)

        if response.status_code != 204 and response.status_code != 409:
            raise RuntimeError(f"Failed to create topic {topic_name}")

    def delete_topic(self, topic: str) -> None:
        tenant, namespace, topic_name = parse_topic_name(topic)

        if tenant != "default":
            raise ValueError(f"Only the default tenant is supported, got {tenant}")
        if namespace != "default":
            raise ValueError(
                f"Only the default namespace is supported, got {namespace}"
            )

        # Make a PUT request to the admin api to delete the topic
        path = f"/admin/v2/persistent/{tenant}/{namespace}/{topic_name}"
        # Force delete the topic
        path += "?force=true"
        url = self._connection_str + path
        response = requests.delete(url)
        if response.status_code != 204 and response.status_code != 409:
            raise RuntimeError(f"Failed to delete topic {topic_name}")
