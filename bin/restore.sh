# #!/bin/bash

# check to see if the docker container called chroma-clickhouse-1 is running
if [ "$(docker inspect -f '{{.State.Running}}' chroma-clickhouse-1)" = "true" ]; then
    echo "chroma-clickhouse-1 is up, proceeding with backup"
else
    echo "chroma-clickhouse-1 is not up"
    exit 1
fi

while [ $# -gt 0 ]; do

   if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
        # echo $1 $2 // Optional to see the parameter:value result
   fi

  shift
done

# if backup name is not provided, exit
if [ -z "$backup_name" ]; then
    echo "backup_name is not provided"
    exit 1
fi

echo $backup_name

# change file permissions to -rw-r-----
chmod 440 ../backups/$backup_name/$backup_name.zip
chmod 777 ../backups/$backup_name/$backup_name.zip
docker cp ../backups/$backup_name/index_data chroma-server-1:/
docker cp ../backups/$backup_name/$backup_name.zip chroma-clickhouse-1:/etc/clickhouse-server/$backup_name.zip

docker exec -u 0 -it chroma-clickhouse-1 chmod 777 /etc/clickhouse-server/$backup_name.zip
docker exec -u 0 -it chroma-clickhouse-1 chown 1001 /etc/clickhouse-server/$backup_name.zip
docker exec -u 0 -it chroma-clickhouse-1 chgrp root /etc/clickhouse-server/$backup_name.zip
docker exec -u 0 -it chroma-clickhouse-1 clickhouse-client --query="DROP TABLE embeddings"
docker exec -u 0 -it chroma-clickhouse-1 clickhouse-client --query="DROP TABLE results"
docker exec -u 0 -it chroma-clickhouse-1 rm -rf /bitnami/clickhouse/data/tmp
docker exec -u 0 -it chroma-clickhouse-1 clickhouse-client --query="RESTORE DATABASE default FROM Disk('backups', '$backup_name.zip')"
