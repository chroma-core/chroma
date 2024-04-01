-- name: GetLastRecordForCollection :one
SELECT id
FROM record_log
WHERE collection_id = $1
ORDER BY id DESC
LIMIT 1;

-- name: InsertRecord :copyfrom
INSERT INTO record_log (collection_id, id, record) values($1, $2, $3);

-- name: GetRecordsForCollection :many
SELECT * FROM record_log WHERE collection_id = $1 AND id > $2  ORDER BY id DESC limit $3 ;

-- name: GetAllCollectionsToCompact :many
with summary as (
    select r.collection_id, r.id, r.timestamp, row_number() over(partition by r.collection_id order by r.id) as rank
    from record_log r, collection_position c
    where r.collection_id = c.collection_id
      and r.id>c.record_log_position
)
select * from summary
where rank=1
order by timestamp;

-- name: UpsertCollectionPosition :exec
INSERT INTO collection_position (collection_id, record_log_position) values($1, $2)
ON CONFLICT (collection_id) DO UPDATE SET record_log_position = $2;