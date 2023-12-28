# Tests the CustomResourceMemberlist provider
import threading
from chromadb.test.conftest import skip_if_not_cluster
from kubernetes import client, config
from chromadb.config import System, Settings
from chromadb.segment.distributed import Memberlist
from chromadb.segment.impl.distributed.segment_directory import (
    CustomResourceMemberlistProvider,
    KUBERNETES_GROUP,
    KUBERNETES_NAMESPACE,
)
import time


# Used for testing to update the memberlist CRD
def update_memberlist(n: int, memberlist_name: str = "test-memberlist") -> Memberlist:
    config.load_config()
    api_instance = client.CustomObjectsApi()

    members = [{"url": f"10.0.0.{i}"} for i in range(1, n + 1)]

    body = {
        "kind": "MemberList",
        "metadata": {"name": memberlist_name},
        "spec": {"members": members},
    }

    _ = api_instance.patch_namespaced_custom_object(
        group=KUBERNETES_GROUP,
        version="v1",
        namespace=KUBERNETES_NAMESPACE,
        plural="memberlists",
        name=memberlist_name,
        body=body,
    )

    return [m["url"] for m in members]


def compare_memberlists(m1: Memberlist, m2: Memberlist) -> bool:
    return sorted(m1) == sorted(m2)


@skip_if_not_cluster()
def test_can_get_memberlist() -> None:
    # This test assumes that the memberlist CRD is already created with the name "test-memberlist"
    system = System(Settings(allow_reset=True))
    provider = system.instance(CustomResourceMemberlistProvider)
    provider.set_memberlist_name("test-memberlist")
    system.reset_state()
    system.start()

    # Update the memberlist
    members = update_memberlist(3)

    # Check that the memberlist is updated after a short delay
    time.sleep(2)
    assert compare_memberlists(provider.get_memberlist(), members)

    system.stop()


@skip_if_not_cluster()
def test_can_update_memberlist_multiple_times() -> None:
    # This test assumes that the memberlist CRD is already created with the name "test-memberlist"
    system = System(Settings(allow_reset=True))
    provider = system.instance(CustomResourceMemberlistProvider)
    provider.set_memberlist_name("test-memberlist")
    system.reset_state()
    system.start()

    # Update the memberlist
    members = update_memberlist(3)

    # Check that the memberlist is updated after a short delay
    time.sleep(2)
    assert compare_memberlists(provider.get_memberlist(), members)

    # Update the memberlist again
    members = update_memberlist(5)

    # Check that the memberlist is updated after a short delay
    time.sleep(2)
    assert compare_memberlists(provider.get_memberlist(), members)

    system.stop()


@skip_if_not_cluster()
def test_stop_memberlist_kills_thread() -> None:
    # This test assumes that the memberlist CRD is already created with the name "test-memberlist"
    system = System(Settings(allow_reset=True))
    provider = system.instance(CustomResourceMemberlistProvider)
    provider.set_memberlist_name("test-memberlist")
    system.reset_state()
    system.start()

    # Make sure a background thread is running
    assert len(threading.enumerate()) == 2

    # Update the memberlist
    members = update_memberlist(3)

    # Check that the memberlist is updated after a short delay
    time.sleep(2)
    assert compare_memberlists(provider.get_memberlist(), members)

    # Stop the system
    system.stop()

    # Check to make sure only one thread is running
    assert len(threading.enumerate()) == 1
