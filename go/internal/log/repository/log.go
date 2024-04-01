package repository

import (
	"context"
	"fmt"
	log "github.com/chroma-core/chroma/go/database/log/db"
	"github.com/jackc/pgx/v5"
)

type LogRepository struct {
	conn    *pgx.Conn
	queries *log.Queries
}

func (r *LogRepository) InsertRecords(ctx context.Context, collectionId string, records [][]byte) (insertCount int64, err error) {
	var lastRecordId int64
	lastRecordId, err = r.queries.GetLastRecordForCollection(ctx, collectionId)
	fmt.Println("INSERT START", collectionId, lastRecordId)
	params := make([]log.InsertRecordParams, len(records))
	for i, record := range records {
		lastRecordId = lastRecordId + 1
		params[i] = log.InsertRecordParams{
			CollectionID: collectionId,
			Record:       record,
			ID:           lastRecordId,
		}
		fmt.Println("INSERT", collectionId, lastRecordId)

	}
	insertCount, err = r.queries.InsertRecord(ctx, params)
	return
}

func (r *LogRepository) PullRecords(ctx context.Context, collectionId string, id int64, batchSize int) (records []log.RecordLog, err error) {
	records, err = r.queries.GetRecordsForCollection(ctx, log.GetRecordsForCollectionParams{
		CollectionID: collectionId,
		ID:           id,
		Limit:        int32(batchSize),
	})
	return
}

func (r *LogRepository) GetAllCollectionInfoToCompact(ctx context.Context) (collectionToCompact []log.GetAllCollectionsToCompactRow, err error) {
	collectionToCompact, err = r.queries.GetAllCollectionsToCompact(ctx)
	if collectionToCompact == nil {
		collectionToCompact = []log.GetAllCollectionsToCompactRow{}
	}
	return
}
func (r *LogRepository) UpdateCollectionPosition(ctx context.Context, collectionId string, position int64) (err error) {
	err = r.queries.UpsertCollectionPosition(ctx, log.UpsertCollectionPositionParams{
		CollectionID:      collectionId,
		RecordLogPosition: position,
	})
	return
}

func NewLogRepository(conn *pgx.Conn) *LogRepository {
	return &LogRepository{
		conn:    conn,
		queries: log.New(conn),
	}
}
