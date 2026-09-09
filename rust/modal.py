import modal, os, signal, subprocess

image = (
    modal.Image.from_dockerfile('rust/Dockerfile.mdac', context='.')
)

app = modal.App("token-bucket-service", image=image)

FORWARD = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGUSR1)

@app.server(port=8000, unauthenticated=True, exit_grace_period=20, startup_timeout=30)
class Server:
    @modal.enter()
    def start(self):
        self.proc = subprocess.Popen(["/usr/local/bin/token_bucket_service"])

        def relay(signum, _frame):
            if self.proc.poll() is None:
                self.proc.send_signal(signum)

        for sig in FORWARD:
            try:
                signal.signal(sig, relay)
            except ValueError:
                pass  # not on the main thread; @modal.exit still covers shutdown

    @modal.exit()
    def stop(self):
        if self.proc.poll() is not None:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=20)   # stay under Modal's 30 s hard limit
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
