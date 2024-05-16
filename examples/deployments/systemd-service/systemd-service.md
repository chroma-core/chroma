# Running Chroma as a systemd service

You can run Chroma as a systemd service which wil allow you to automatically start Chroma on boot and restart it if it
crashes.

### Docker Compose

Create a file `/etc/systemd/system/chroma.service` with the following content:

> Note: The below example assumes Debian-based system with docker-ce installed.

```ini
[Unit]
Description = Chroma Docker Service
After = network.target docker.service
Requires = docker.service

[Service]
Type = forking
User = root
Group = root
WorkingDirectory = /home/admin/chroma
ExecStart = /usr/bin/docker compose up -d
ExecStop = /usr/bin/docker compose down
RemainAfterExit = true

[Install]
WantedBy = multi-user.target
```

In the above example adjust the `User`, `Group`, `WorkingDirectory`, `ExecStart` (docker executable location)
and `ExecStop` as per your setup.

Alternatively you can copy the [chroma-docker.service](chroma-docker.service) file
to `/etc/systemd/system/chroma.service` or use `wget`:

```bash
wget https://raw.githubusercontent.com/chroma-core/chroma/main/examples/deployments/systemd-service/chroma-docker.service \
  -O /etc/systemd/system/chroma.service
```

Loading, enabling and starting the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable chroma
sudo systemctl start chroma
```

### Chroma CLI

To run Chroma using the Chroma CLI, you can follow the below steps.

Create a file `/etc/systemd/system/chroma.service` with the following content:

> Note: The below example assumes that Chroma is installed in Python `site-packages` package.

```ini
[Unit]
Description = Chroma Service
After = network.target

[Service]
Type = simple
User = root
Group = root
WorkingDirectory = /chroma
ExecStart = /usr/local/bin/chroma run --host 127.0.0.1 --port 8000 --path /chroma/data --log-path /var/log/chroma.log

[Install]
WantedBy = multi-user.target
```

In the above example adjust the `User`, `Group`, `WorkingDirectory`, and `ExecStart` (chroma cli script) as per your
setup. The `--host`, `--port`, `--path` might also need to be adjusted as per your setup.

Alternatively you can copy the [chroma-docker.service](chroma-docker.service) file
to `/etc/systemd/system/chroma.service` or use `wget`:

```bash
wget https://raw.githubusercontent.com/chroma-core/chroma/main/examples/deployments/systemd-service/chroma-cli.service \
  -O /etc/systemd/system/chroma.service
```

Loading, enabling and starting the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable chroma
sudo systemctl start chroma
```
