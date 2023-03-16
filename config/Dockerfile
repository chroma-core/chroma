FROM clickhouse/clickhouse-server:22.9-alpine
ADD ./config/backup_disk.xml /etc/clickhouse-server/config.d/backup_disk.xml
ADD ./config/chroma_users.xml /etc/clickhouse-server/users.d/chroma.xml
