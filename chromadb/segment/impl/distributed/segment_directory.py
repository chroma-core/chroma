from typing import Any, Callable, Dict, Optional, cast
from overrides import EnforceOverrides, override
from chromadb.config import System
from chromadb.segment.distributed import (
    Memberlist,
    MemberlistProvider,
    SegmentDirectory,
)
from chromadb.types import Segment
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
import threading
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    add_attributes_to_current_span,
    trace_method,
)
import time

from chromadb.utils.rendezvous_hash import assign, murmur3hasher

# These could go in config but given that they will rarely change, they are here for now to avoid
# polluting the config file further.
WATCH_TIMEOUT_SECONDS = 60
KUBERNETES_NAMESPACE = "chroma"
KUBERNETES_GROUP = "chroma.cluster"
HEADLESS_SERVICE = "svc.cluster.local"


class MockMemberlistProvider(MemberlistProvider, EnforceOverrides):
    """A mock memberlist provider for testing"""

    _memberlist: Memberlist

    def __init__(self, system: System):
        super().__init__(system)
        self._memberlist = ["a", "b", "c"]

    @override
    def get_memberlist(self) -> Memberlist:
        return self._memberlist

    @override
    def set_memberlist_name(self, memberlist: str) -> None:
        pass  # The mock provider does not need to set the memberlist name

    def update_memberlist(self, memberlist: Memberlist) -> None:
        """Updates the memberlist and calls all registered callbacks. This mocks an update from a k8s CR"""
        self._memberlist = memberlist
        for callback in self.callbacks:
            callback(memberlist)


class CustomResourceMemberlistProvider(MemberlistProvider, EnforceOverrides):
    """A memberlist provider that uses a k8s custom resource to store the memberlist"""

    _kubernetes_api: client.CustomObjectsApi
    _memberlist_name: Optional[str]
    _curr_memberlist: Optional[Memberlist]
    _curr_memberlist_mutex: threading.Lock
    _watch_thread: Optional[threading.Thread]
    _kill_watch_thread: threading.Event
    _done_waiting_for_reset: threading.Event

    def __init__(self, system: System):
        super().__init__(system)
        config.load_config()
        self._kubernetes_api = client.CustomObjectsApi()
        self._watch_thread = None
        self._memberlist_name = None
        self._curr_memberlist = None
        self._curr_memberlist_mutex = threading.Lock()
        self._kill_watch_thread = threading.Event()
        self._done_waiting_for_reset = threading.Event()

    @override
    def start(self) -> None:
        if self._memberlist_name is None:
            raise ValueError("Memberlist name must be set before starting")
        self.get_memberlist()
        self._done_waiting_for_reset.clear()
        self._watch_worker_memberlist()
        return super().start()

    @override
    def stop(self) -> None:
        self._curr_memberlist = None
        self._memberlist_name = None

        # Stop the watch thread
        self._kill_watch_thread.set()
        if self._watch_thread is not None:
            self._watch_thread.join()
        self._watch_thread = None
        self._kill_watch_thread.clear()
        self._done_waiting_for_reset.clear()
        return super().stop()

    @override
    def reset_state(self) -> None:
        # Reset the memberlist in kubernetes, and wait for it to
        # get propagated back again
        # Note that the component must be running in order to reset the state
        if not self._system.settings.require("allow_reset"):
            raise ValueError(
                "Resetting the database is not allowed. Set `allow_reset` to true in the config in tests or other non-production environments where reset should be permitted."
            )
        if self._memberlist_name:
            self._done_waiting_for_reset.clear()
            self._kubernetes_api.patch_namespaced_custom_object(
                group=KUBERNETES_GROUP,
                version="v1",
                namespace=KUBERNETES_NAMESPACE,
                plural="memberlists",
                name=self._memberlist_name,
                body={
                    "kind": "MemberList",
                    "spec": {"members": []},
                },
            )
            self._done_waiting_for_reset.wait(5.0)
            # TODO: For some reason the above can flake and the memberlist won't be populated
            # Given that this is a test harness, just sleep for an additional 500ms for now
            # We should understand why this flaps
            time.sleep(0.5)

    @override
    def get_memberlist(self) -> Memberlist:
        if self._curr_memberlist is None:
            self._curr_memberlist = self._fetch_memberlist()
        return self._curr_memberlist

    @override
    def set_memberlist_name(self, memberlist: str) -> None:
        self._memberlist_name = memberlist

    def _fetch_memberlist(self) -> Memberlist:
        api_response = self._kubernetes_api.get_namespaced_custom_object(
            group=KUBERNETES_GROUP,
            version="v1",
            namespace=KUBERNETES_NAMESPACE,
            plural="memberlists",
            name=f"{self._memberlist_name}",
        )
        api_response = cast(Dict[str, Any], api_response)
        if "spec" not in api_response:
            return []
        response_spec = cast(Dict[str, Any], api_response["spec"])
        return self._parse_response_memberlist(response_spec)

    def _watch_worker_memberlist(self) -> None:
        # TODO: We may want to make this watch function a library function that can be used by other
        # components that need to watch k8s custom resources.
        def run_watch() -> None:
            w = watch.Watch()

            def do_watch() -> None:
                for event in w.stream(
                    self._kubernetes_api.list_namespaced_custom_object,
                    group=KUBERNETES_GROUP,
                    version="v1",
                    namespace=KUBERNETES_NAMESPACE,
                    plural="memberlists",
                    field_selector=f"metadata.name={self._memberlist_name}",
                    timeout_seconds=WATCH_TIMEOUT_SECONDS,
                ):
                    event = cast(Dict[str, Any], event)
                    response_spec = event["object"]["spec"]
                    response_spec = cast(Dict[str, Any], response_spec)
                    with self._curr_memberlist_mutex:
                        self._curr_memberlist = self._parse_response_memberlist(
                            response_spec
                        )
                    self._notify(self._curr_memberlist)
                    if (
                        self._system.settings.require("allow_reset")
                        and not self._done_waiting_for_reset.is_set()
                        and len(self._curr_memberlist) > 0
                    ):
                        self._done_waiting_for_reset.set()

            # Watch the custom resource for changes
            # Watch with a timeout and retry so we can gracefully stop this if needed
            while not self._kill_watch_thread.is_set():
                try:
                    do_watch()
                except ApiException as e:
                    # If status code is 410, the watch has expired and we need to start a new one.
                    if e.status == 410:
                        pass
            return

        if self._watch_thread is None:
            thread = threading.Thread(target=run_watch, daemon=True)
            thread.start()
            self._watch_thread = thread
        else:
            raise Exception("A watch thread is already running.")

    def _parse_response_memberlist(
        self, api_response_spec: Dict[str, Any]
    ) -> Memberlist:
        if "members" not in api_response_spec:
            return []
        return [m["member_id"] for m in api_response_spec["members"]]

    def _notify(self, memberlist: Memberlist) -> None:
        for callback in self.callbacks:
            callback(memberlist)


class RendezvousHashSegmentDirectory(SegmentDirectory, EnforceOverrides):
    _memberlist_provider: MemberlistProvider
    _curr_memberlist_mutex: threading.Lock
    _curr_memberlist: Optional[Memberlist]

    def __init__(self, system: System):
        super().__init__(system)
        self._memberlist_provider = self.require(MemberlistProvider)
        memberlist_name = system.settings.require("worker_memberlist_name")
        self._memberlist_provider.set_memberlist_name(memberlist_name)

        self._curr_memberlist = None
        self._curr_memberlist_mutex = threading.Lock()

    @override
    def start(self) -> None:
        self._curr_memberlist = self._memberlist_provider.get_memberlist()
        self._memberlist_provider.register_updated_memberlist_callback(
            self._update_memberlist
        )
        return super().start()

    @override
    def stop(self) -> None:
        self._memberlist_provider.unregister_updated_memberlist_callback(
            self._update_memberlist
        )
        return super().stop()

    @override
    def get_segment_endpoint(self, segment: Segment) -> str:
        if self._curr_memberlist is None or len(self._curr_memberlist) == 0:
            raise ValueError("Memberlist is not initialized")
        assignment = assign(segment["id"].hex, self._curr_memberlist, murmur3hasher)
        service_name = self.extract_service_name(assignment)
        assignment = f"{assignment}.{service_name}.{KUBERNETES_NAMESPACE}.{HEADLESS_SERVICE}:50051"  # TODO: make port configurable
        return assignment

    @override
    def register_updated_segment_callback(
        self, callback: Callable[[Segment], None]
    ) -> None:
        raise NotImplementedError()

    @trace_method(
        "RendezvousHashSegmentDirectory._update_memberlist",
        OpenTelemetryGranularity.ALL,
    )
    def _update_memberlist(self, memberlist: Memberlist) -> None:
        with self._curr_memberlist_mutex:
            add_attributes_to_current_span({"new_memberlist": memberlist})
            self._curr_memberlist = memberlist

    def extract_service_name(self, pod_name: str) -> Optional[str]:
        # Split the pod name by the hyphen
        parts = pod_name.split("-")
        # The service name is expected to be the prefix before the last hyphen
        if len(parts) > 1:
            return "-".join(parts[:-1])
        return None
