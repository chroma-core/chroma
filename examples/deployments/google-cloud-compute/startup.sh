#! /bin/bash

cd ~

apt-get update -y
apt-get install -y ca-certificates curl gnupg lsb-release
mkdir -m 0755 -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update -y
chmod a+r /etc/apt/keyrings/docker.gpg
apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

cat << EOF > docker-compose.yaml
version: "3.9"
services:
  server:
    container_name: server
    image: ghcr.io/chroma-core/chroma:0.3.14
    volumes:
      - ./index_data:/index_data
    environment:
      - CHROMA_DB_IMPL=clickhouse
      - CLICKHOUSE_HOST=clickhouse
      - CLICKHOUSE_PORT=8123
    ports:
      - '8000:8000'
    depends_on:
      - clickhouse
  clickhouse:
    container_name: clickhouse
    image: clickhouse/clickhouse-server:22.9-alpine
    volumes:
      - ./clickhouse_data:/bitnami/clickhouse
      - ./backups:/backups
      - ./config/backup_disk.xml:/etc/clickhouse-server/config.d/backup_disk.xml
      - ./config/chroma_users.xml:/etc/clickhouse-server/users.d/chroma.xml
    environment:
      - ALLOW_EMPTY_PASSWORD=yes
      - CLICKHOUSE_TCP_PORT=9000
      - CLICKHOUSE_HTTP_PORT=8123
    ports:
      - '8123:8123'
      - '9000:9000'
EOF

mkdir config
cat << EOF > config/backup_disk.xml
<clickhouse>
    <storage_configuration>
        <disks>
            <backups>
                <type>local</type>
                <path>/etc/clickhouse-server/</path>
            </backups>
        </disks>
    </storage_configuration>
    <backups>
        <allowed_disk>backups</allowed_disk>
        <allowed_path>/etc/clickhouse-server/</allowed_path>
    </backups>
</clickhouse>
EOF

cat << EOF > config/chroma_users.xml
<clickhouse>
    <profiles>
        <default>
            <allow_experimental_lightweight_delete>1</allow_experimental_lightweight_delete>
            <mutations_sync>1</mutations_sync>
        </default>
    </profiles>
</clickhouse>
EOF

COMPOSE_PROJECT_NAME=chroma docker compose up -d
