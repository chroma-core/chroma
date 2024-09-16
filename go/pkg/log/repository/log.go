package repository

import (
	"context"
	"errors"
	"time"

	log "github.com/chroma-core/chroma/go/database/log/db"
	"github.com/chroma-core/chroma/go/pkg/log/sysdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	trace_log "github.com/pingcap/log"
	"go.uber.org/zap"
)

type LogRepository struct {
	conn    *pgxpool.Pool
	queries *log.Queries
	sysDb   sysdb.ISysDB
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
		trace_log.Error("Error in fetching collection from collection table", zap.Error(err), zap.String("collectionId", collectionId))
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
	trace_log.Info("Updated record_enumeration_offset_position in the collection table", zap.Int64("offsetPosition", collection.RecordEnumerationOffsetPosition+insertCount), zap.String("collectionId", collectionId))
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
	if err != nil {
		trace_log.Error("Error in getting collections to compact from record_log table", zap.Error(err))
	} else {
		trace_log.Info("Got collections to compact from record_log table", zap.Int("collectionCount", len(collectionToCompact)))
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
	trace_log.Info("Updated record_compaction_offset_position in the collection table", zap.Int64("offsetPosition", offsetPosition), zap.String("collectionId", collectionId))
	return
}

func (r *LogRepository) PurgeRecords(ctx context.Context) (err error) {
	trace_log.Info("Purging records from record_log table")
	err = r.queries.PurgeRecords(ctx)
	return
}

func (r *LogRepository) GetTotalUncompactedRecordsCount(ctx context.Context) (totalUncompactedDepth int64, err error) {
	totalUncompactedDepth, err = r.queries.GetTotalUncompactedRecordsCount(ctx)
	if err != nil {
		trace_log.Error("Error in getting total uncompacted records count from collection table", zap.Error(err))
	}
	return
}

func (r *LogRepository) GarbageCollection(ctx context.Context) error {
	collectionToCompact, err := r.queries.GetAllCollections(ctx)
	if err != nil {
		trace_log.Error("Error in getting collections to compact", zap.Error(err))
		return err
	} else {
		trace_log.Info("GC Got collections to compact", zap.Int("collectionCount", len(collectionToCompact)))
	}
	if collectionToCompact == nil {
		return nil
	}
	collectionsToGC := make([]string, 0)
	for _, collection := range collectionToCompact {
		exist, err := r.sysDb.CheckCollection(ctx, collection)
		if err != nil {
			trace_log.Error("Error in checking collection in sysdb", zap.Error(err), zap.String("collectionId", collection))
			continue
		}
		if !exist {
			collectionsToGC = append(collectionsToGC, collection)
		}
	}
	if len(collectionsToGC) > 0 {
		trace_log.Info("Collections to be garbage collected", zap.Strings("collections", collectionsToGC))
		var tx pgx.Tx
		tx, err = r.conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			trace_log.Error("Error in begin transaction for garbage collection", zap.Error(err))
			return err
		}
		queriesWithTx := r.queries.WithTx(tx)
		defer func() {
			if err != nil {
				tx.Rollback(ctx)
			} else {
				err = tx.Commit(ctx)
			}
		}()
		trace_log.Info("Starting garbage collection", zap.Strings("collections", collectionsToGC))
		err = queriesWithTx.DeleteRecords(ctx, collectionsToGC)
		if err != nil {
			trace_log.Error("Error in garbage collection", zap.Error(err))
			return err
		}
		trace_log.Info("Delete collections", zap.Strings("collections", collectionsToGC))
		err = queriesWithTx.DeleteCollection(ctx, collectionsToGC)
		if err != nil {
			trace_log.Error("Error in deleting collection", zap.Error(err))
			return err
		}
		trace_log.Info("Garbage collection completed", zap.Strings("collections", collectionsToGC))
	}
	return nil
}

func NewLogRepository(conn *pgxpool.Pool, sysDb sysdb.ISysDB) *LogRepository {
	return &LogRepository{
		conn:    conn,
		queries: log.New(conn),
		sysDb:   sysDb,
	}
}
