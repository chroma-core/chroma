from abc import abstractmethod
import os
from typing import Callable, ClassVar, Dict, Any
import uuid
import time
from threading import Event, Thread
import chromadb
from chromadb.config import Component
from pathlib import Path
from enum import Enum

TELEMETRY_WHITELISTED_SETTINGS = [
    "chroma_api_impl",
    "is_persistent",
    "chroma_server_ssl_enabled",
]


class ServerContext(Enum):
    NONE = "None"
    FASTAPI = "FastAPI"


class TelemetryEvent:
    max_batch_size: ClassVar[int] = 1
    batch_size: int

    def __init__(self, batch_size: int = 1):
        self.batch_size = batch_size

    @property
    def properties(self) -> Dict[str, Any]:
        return self.__dict__

    @property
    def name(self) -> str:
        return self.__class__.__name__

    # A batch key is used to determine whether two events can be batched together.
    # If a TelemetryEvent's max_batch_size > 1, batch_key() and batch() MUST be implemented.
    # Otherwise they are ignored.
    @property
    def batch_key(self) -> str:
        return self.name

    def batch(self, other: "TelemetryEvent") -> "TelemetryEvent":
        raise NotImplementedError


class RepeatedTelemetry:
    def __init__(self, interval: int, function: Callable[[], None]):
        self.interval = interval
        self.function = function
        self.start = time.time()
        self.event = Event()
        self.thread = Thread(target=self._target)
        self.thread.daemon = True
        self.thread.start()

    def _target(self) -> None:
        while not self.event.wait(self._time):
            self.function()

    @property
    def _time(self) -> float:
        return self.interval - ((time.time() - self.start) % self.interval)

    def stop(self) -> None:
        self.event.set()
        self.thread.join()


class Telemetry(Component):
    USER_ID_PATH = str(Path.home() / ".cache" / "chroma" / "telemetry_user_id")
    UNKNOWN_USER_ID = "UNKNOWN"
    SERVER_CONTEXT: ServerContext = ServerContext.NONE
    _curr_user_id = None

    @abstractmethod
    def capture(self, event: TelemetryEvent) -> None:
        pass

    # Schedule a function that creates a TelemetryEvent to be called every `every_seconds` seconds.
    def schedule_event_function(
        self, event_function: Callable[..., TelemetryEvent], every_seconds: int
    ) -> None:
        RepeatedTelemetry(every_seconds, lambda: self.capture(event_function()))

    @property
    def context(self) -> Dict[str, Any]:
        chroma_version = chromadb.__version__
        settings = chromadb.get_settings()
        telemetry_settings = {}
        for whitelisted in TELEMETRY_WHITELISTED_SETTINGS:
            telemetry_settings[whitelisted] = settings[whitelisted]

        self._context = {
            "chroma_version": chroma_version,
            "server_context": self.SERVER_CONTEXT.value,
            **telemetry_settings,
        }
        return self._context

    @property
    def user_id(self) -> str:
        if self._curr_user_id:
            return self._curr_user_id

        # File access may fail due to permissions or other reasons. We don't want to crash so we catch all exceptions.
        try:
            if not os.path.exists(self.USER_ID_PATH):
                os.makedirs(os.path.dirname(self.USER_ID_PATH), exist_ok=True)
                with open(self.USER_ID_PATH, "w") as f:
                    new_user_id = str(uuid.uuid4())
                    f.write(new_user_id)
                self._curr_user_id = new_user_id
            else:
                with open(self.USER_ID_PATH, "r") as f:
                    self._curr_user_id = f.read()
        except Exception:
            self._curr_user_id = self.UNKNOWN_USER_ID
        return self._curr_user_id
