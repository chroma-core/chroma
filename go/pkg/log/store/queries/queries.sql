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
    and (c.record_enumeration_offset_position - c.record_compaction_offset_position) >= sqlc.arg(min_compaction_size)
    and not c.is_sealed
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
DELETE FROM record_log r using collection c where r.collection_id = c.id and r.offset <= c.record_compaction_offset_position;

-- name: GetTotalUncompactedRecordsCount :one
SELECT CAST(COALESCE(SUM(record_enumeration_offset_position - record_compaction_offset_position), 0) AS bigint) AS total_uncompacted_depth FROM collection;

-- name: DeleteRecordsRange :exec
DELETE FROM record_log r where r.collection_id = sqlc.arg(collection_id) and r.offset >= sqlc.arg(min_offset) and r.offset <= sqlc.arg(max_offset);

-- name: GetMinimumMaximumOffsetForCollection :one
SELECT CAST(COALESCE(MIN(r.offset), 0) as bigint) AS min_offset, CAST(COALESCE(MAX(r.offset), 0) as bigint) AS max_offset
FROM record_log r
WHERE r.collection_id = $1;

-- name: GetBoundsForCollection :one
SELECT CAST(COALESCE(MIN(record_compaction_offset_position), 0) as bigint) AS record_compaction_offset_position, CAST(COALESCE(MAX(record_enumeration_offset_position), 0) as bigint) AS record_enumeration_offset_position
FROM collection
WHERE id = $1;

-- name: DeleteCollection :exec
DELETE FROM collection c where c.id = ANY(@collection_ids::text[]);

-- name: GetAllCollections :many
SELECT id FROM collection;

-- name: GetLastCompactedOffset :one
SELECT record_compaction_offset_position FROM collection c WHERE c.id = $1;

-- name: ForkCollectionRecord :exec
INSERT INTO record_log ("offset", collection_id, timestamp, record)
    SELECT record_log.offset, $2, record_log.timestamp, record_log.record
    FROM record_log
    WHERE record_log.collection_id = $1;

-- name: SealLog :one
UPDATE collection SET is_sealed = true WHERE id = $1 returning *;

-- name: SealLogInsert :one
INSERT INTO collection(id, is_sealed, record_compaction_offset_position, record_enumeration_offset_position) VALUES ($1, true, 0, 0) returning *;
