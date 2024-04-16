-- name: GetCollectionForUpdate :one
SELECT *
FROM collection
WHERE id = $1
FOR UPDATE;

-- name: InsertRecord :copyfrom
INSERT INTO record_log (collection_id, "offset", record, timestamp) values($1, $2, $3, $4);

-- name: GetRecordsForCollection :many
SELECT * FROM record_log r WHERE r.collection_id = $1 AND r.offset >= $2 and r.timestamp <= $4  ORDER BY r.offset ASC limit $3 ;

-- name: GetAllCollectionsToCompact :many
with summary as (
    select r.collection_id, r.offset, r.timestamp, row_number() over(partition by r.collection_id order by r.offset) as rank
    from record_log r, collection c
    where r.collection_id = c.id
    and r.offset > c.record_compaction_offset_position
)
select * from summary
where rank=1
order by timestamp;

-- name: UpdateCollectionCompactionOffsetPosition :exec
UPDATE collection set record_compaction_offset_position = $2 where id = $1;

-- name: UpdateCollectionEnumerationOffsetPosition :exec
UPDATE collection set record_enumeration_offset_position = $2 where id = $1;

-- name: InsertCollection :one
INSERT INTO collection (id, record_enumeration_offset_position, record_compaction_offset_position) values($1, $2, $3) returning *;

-- name: PurgeRecords :exec
DELETE FROM record_log r using collection c where r.collection_id = c.id and r.offset < c.record_compaction_offset_position;