# Running Chroma as a systemd service

You can run Chroma as a systemd service which wil allow you to automatically start Chroma on boot and restart it if it
crashes.

Create a file `/etc/systemd/system/chroma.service` with the following content:

!!! note "Example assumptions"

    The below example assumes Debian-based system with docker-ce installed.

```ini
[Unit]
Description = Chroma Service
After = network.target docker.service
Requires = docker.service

[Service]
Type = oneshot
WorkingDirectory = /home/admin/chroma
ExecStart = /usr/bin/docker compose up -d
ExecStop = /usr/bin/docker compose down
RemainAfterExit = true

[Install]
WantedBy = multi-user.target
```

Replace `/home/admin/chroma` with the path to your docker compose is.

Loading, enabling and starting the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable chroma
sudo systemctl start chroma
```
