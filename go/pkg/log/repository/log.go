package repository

import (
	"context"
	"errors"
	"time"

	log "github.com/chroma-core/chroma/go/database/log/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	trace_log "github.com/pingcap/log"
	"go.uber.org/zap"
)

type LogRepository struct {
	conn    *pgxpool.Pool
	queries *log.Queries
}

func (r *LogRepository) InsertRecords(ctx context.Context, collectionId string, records [][]byte) (insertCount int64, err error) {
	var tx pgx.Tx
	tx, err = r.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		trace_log.Error("Error in begin transaction for inserting records to log service", zap.Error(err), zap.String("collectionId", collectionId))
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
		trace_log.Error("Error in fetching collection from collection table", zap.Error(err))
		// If no row found, insert one.
		if errors.Is(err, pgx.ErrNoRows) {
			trace_log.Info("No rows found in the collection table for collection", zap.String("collectionId", collectionId))
			collection, err = queriesWithTx.InsertCollection(ctx, log.InsertCollectionParams{
				ID:                              collectionId,
				RecordEnumerationOffsetPosition: 0,
				RecordCompactionOffsetPosition:  0,
			})
			if err != nil {
				trace_log.Error("Error in creating a new entry in collection table", zap.Error(err), zap.String("collectionId", collectionId))
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
		trace_log.Error("Error in inserting records to record_log table", zap.Error(err), zap.String("collectionId", collectionId))
		return
	}
	trace_log.Info("Inserted records to record_log table", zap.Int64("recordCount", insertCount), zap.String("collectionId", collectionId))
	err = queriesWithTx.UpdateCollectionEnumerationOffsetPosition(ctx, log.UpdateCollectionEnumerationOffsetPositionParams{
		ID:                              collectionId,
		RecordEnumerationOffsetPosition: collection.RecordEnumerationOffsetPosition + insertCount,
	})
	if err != nil {
		trace_log.Error("Error in updating record_enumeration_offset_position in the collection table", zap.Error(err), zap.String("collectionId", collectionId))
	}
	return
}

func (r *LogRepository) PullRecords(ctx context.Context, collectionId string, offset int64, batchSize int, timestamp int64) (records []log.RecordLog, err error) {
	records, err = r.queries.GetRecordsForCollection(ctx, log.GetRecordsForCollectionParams{
		CollectionID: collectionId,
		Offset:       offset,
		Limit:        int32(batchSize),
		Timestamp:    timestamp,
	})
	if err != nil {
		trace_log.Error("Error in pulling records from record_log table", zap.Error(err), zap.String("collectionId", collectionId))
	}
	return
}

func (r *LogRepository) GetAllCollectionInfoToCompact(ctx context.Context, minCompactionSize uint64) (collectionToCompact []log.GetAllCollectionsToCompactRow, err error) {
	collectionToCompact, err = r.queries.GetAllCollectionsToCompact(ctx, int64(minCompactionSize))
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
	if err != nil {
		trace_log.Error("Error in updating record_compaction_offset_position in the collection table", zap.Error(err), zap.String("collectionId", collectionId))
	}
	trace_log.Info("Updated record_compaction_offset_position in the collection table", zap.Int64("recordCount", offsetPosition), zap.String("collectionId", collectionId))
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
