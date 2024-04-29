package repository

import (
	"context"
	"errors"
	log "github.com/chroma-core/chroma/go/database/log/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type LogRepository struct {
	conn    *pgxpool.Pool
	queries *log.Queries
}

func (r *LogRepository) InsertRecords(ctx context.Context, collectionId string, records [][]byte) (insertCount int64, err error) {
	var tx pgx.Tx
	tx, err = r.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return
	}
	var collection log.Collection
	queriesWithTx := r.queries.WithTx(tx)
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()
	collection, err = queriesWithTx.GetCollectionForUpdate(ctx, collectionId)
	if err != nil {
		// If no row found, insert one.
		if errors.Is(err, pgx.ErrNoRows) {
			collection, err = queriesWithTx.InsertCollection(ctx, log.InsertCollectionParams{
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
		offset := collection.RecordEnumerationOffsetPosition + int64(i) + 1
		params[i] = log.InsertRecordParams{
			CollectionID: collectionId,
			Record:       record,
			Offset:       offset,
			Timestamp:    time.Now().UnixNano(),
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

func (r *LogRepository) PullRecords(ctx context.Context, collectionId string, offset int64, batchSize int, timestamp int64) (records []log.RecordLog, err error) {
	records, err = r.queries.GetRecordsForCollection(ctx, log.GetRecordsForCollectionParams{
		CollectionID: collectionId,
		Offset:       offset,
		Limit:        int32(batchSize),
		Timestamp:    timestamp,
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

func (r *LogRepository) PurgeRecords(ctx context.Context) (err error) {
	err = r.queries.PurgeRecords(ctx)
	return
}

func NewLogRepository(conn *pgxpool.Pool) *LogRepository {
	return &LogRepository{
		conn:    conn,
		queries: log.New(conn),
	}
}
