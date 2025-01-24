from chromadb.api import ServerAPI
from chromadb.config import System
import time


# TODO: this should be removed once RustBindingsAPI is part of our parameterized tests
def test_heartbeat(rust_system: System) -> None:
    nanoseconds_since_epoch = time.time_ns()
    server = rust_system.require(ServerAPI)
    assert server.heartbeat() >= nanoseconds_since_epoch
