# #!/bin/bash

# # check to see if the docker container called chroma-private-clickhouse-1 is running
# if [ "$(docker inspect -f '{{.State.Running}}' chroma-private-clickhouse-1)" = "true" ]; then
#     echo "chroma-private-clickhouse-1 is up, proceeding with backup"
# else
#     echo "chroma-private-clickhouse-1 is not up"
#     exit 1
# fi

# backup_name=${backup_name:-backup}
# backup_name="$backup_name-$(date +%Y_%m_%d-%H_%M_%S)"

# # date with format YYYY_MM_DD-HH_MM_SS
# # backup_date=$(date +%Y_%m_%d-%H_%M_%S)

# while [ $# -gt 0 ]; do

#    if [[ $1 == *"--"* ]]; then
#         param="${1/--/}"
#         declare $param="$2"
#         # echo $1 $2 // Optional to see the parameter:value result
#    fi

#   shift
# done

# echo $backup_name

# # create a folder at ../backup to store the backup
# mkdir -p ../backups

# # create a folder inside of ../backups with the name of the backup
# mkdir -p ../backups/$backup_name

# # create folder in ../backups with that name string, if folder already exists, exit
# docker exec -u 0 -it chroma-private-clickhouse-1 clickhouse-client --query="BACKUP DATABASE default TO Disk('backups', '$backup_name.zip')"

# # use that name to dump the clickhouse db and copy into the folder
# docker cp chroma-private-clickhouse-1:/etc/clickhouse-server/$backup_name.zip ../backups/$backup_name/$backup_name.zip

# # remove the backup from teh clickhouse container
# docker exec -u 0 -it chroma-private-clickhouse-1 rm /etc/clickhouse-server/$backup_name.zip

# # copy the entire contents of
# docker cp chroma-private-server-1:/index_data ../backups/$backup_name
