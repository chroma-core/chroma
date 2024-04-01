package repository

import (
	"context"
	"errors"
	log "github.com/chroma-core/chroma/go/database/log/db"
	"github.com/jackc/pgx/v5"
)

type LogRepository struct {
	conn    *pgx.Conn
	queries *log.Queries
}

func (r *LogRepository) InsertRecords(ctx context.Context, collectionId string, records [][]byte) (insertCount int64, err error) {
	var tx pgx.Tx
	tx, err = r.conn.BeginTx(ctx, pgx.TxOptions{})
	var collection log.Collection
	queriesWithTx := r.queries.WithTx(tx)
	defer func() {
		if err != nil {
			err = tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()
	collection, err = queriesWithTx.GetCollectionForUpdate(ctx, collectionId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = queriesWithTx.InsertCollection(ctx, log.InsertCollectionParams{
				ID:                              collectionId,
				RecordEnumerationOffsetPosition: 0,
				RecordCompactionOffsetPosition:  0,
			})
			if err != nil {
				return
			}
		} else {
			return
		}
	}
	params := make([]log.InsertRecordParams, len(records))
	for i, record := range records {
		params[i] = log.InsertRecordParams{
			CollectionID: collectionId,
			Record:       record,
			Offset:       collection.RecordEnumerationOffsetPosition + int64(i) + 1,
		}
	}
	insertCount, err = queriesWithTx.InsertRecord(ctx, params)
	if err != nil {
		return
	}
	err = queriesWithTx.UpdateCollectionEnumerationOffsetPosition(ctx, log.UpdateCollectionEnumerationOffsetPositionParams{
		ID:                              collectionId,
		RecordEnumerationOffsetPosition: collection.RecordEnumerationOffsetPosition + insertCount,
	})
	return
}

func (r *LogRepository) PullRecords(ctx context.Context, collectionId string, offset int64, batchSize int) (records []log.RecordLog, err error) {
	records, err = r.queries.GetRecordsForCollection(ctx, log.GetRecordsForCollectionParams{
		CollectionID: collectionId,
		Offset:       offset,
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
func (r *LogRepository) UpdateCollectionCompactionOffsetPosition(ctx context.Context, collectionId string, offsetPosition int64) (err error) {
	err = r.queries.UpdateCollectionCompactionOffsetPosition(ctx, log.UpdateCollectionCompactionOffsetPositionParams{
		ID:                             collectionId,
		RecordCompactionOffsetPosition: offsetPosition,
	})
	return
}

func NewLogRepository(conn *pgx.Conn) *LogRepository {
	return &LogRepository{
		conn:    conn,
		queries: log.New(conn),
	}
}
