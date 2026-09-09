import signal
import subprocess
from types import FrameType
from typing import Optional

import modal

image = modal.Image.from_dockerfile(
    "rust/Dockerfile.mdac",
    context_dir=".",
    add_python="3.12",
)

app = modal.App("token-bucket-service")

FORWARD = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGUSR1)


@app.cls(image=image, min_containers=1, max_containers=1)
class Server:
    @modal.enter()  # type: ignore[misc]
    def start(self) -> None:
        self.proc = subprocess.Popen(["/usr/local/bin/token_bucket_service"])

        def relay(signum: int, _frame: Optional[FrameType]) -> None:
            if self.proc.poll() is None:
                self.proc.send_signal(signum)

        for sig in FORWARD:
            try:
                signal.signal(sig, relay)
            except ValueError:
                pass  # not on the main thread; @modal.exit still covers shutdown

    @modal.web_server(  # type: ignore[misc]
        8000, startup_timeout=30, requires_proxy_auth=False
    )
    def serve(self) -> None:
        pass

    @modal.exit()  # type: ignore[misc]
    def stop(self) -> None:
        if self.proc.poll() is not None:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=20)  # stay under Modal's 30 s hard limit
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
