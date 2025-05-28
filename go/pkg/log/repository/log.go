package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/chroma-core/chroma/go/pkg/log/store/db"
	"github.com/chroma-core/chroma/go/pkg/log/sysdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	trace_log "github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LogRepository struct {
	conn    *pgxpool.Pool
	queries *log.Queries
	sysDb   sysdb.ISysDB
}

func (r *LogRepository) InsertRecords(ctx context.Context, collectionId string, records [][]byte) (insertCount int64, isSealed bool, err error) {
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
				var pgErr *pgconn.PgError
				// This is a retryable error and should be retried by upstream. It happens
				// when two concurrent adds to the same collection happen.
				if errors.As(err, &pgErr) && pgErr.Code == "23505" {
					trace_log.Error("Duplicate key error while inserting into collection table", zap.String("collectionId", collectionId), zap.String("detail", pgErr.Detail))
					err = status.Error(codes.AlreadyExists, fmt.Sprintf("Duplicate key error while inserting into collection table: %s", pgErr.Detail))
					return
				}
				trace_log.Error("Error in creating a new entry in collection table", zap.Error(err), zap.String("collectionId", collectionId))
				return
			}
		} else {
			return
		}
	}
	if collection.IsSealed {
		insertCount = 0
		isSealed = true
		err = nil
		return
	}
	isSealed = false
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
		var pgErr *pgconn.PgError
		// This is a retryable error and should be retried by upstream. It happens
		// when two concurrent adds to the same collection happen.
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			trace_log.Error("Duplicate key error while inserting into record_log", zap.String("collectionId", collectionId), zap.String("detail", pgErr.Detail))
			err = status.Error(codes.AlreadyExists, fmt.Sprintf("Duplicate key error while inserting into record_log: %s", pgErr.Detail))
			return
		}
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
		return
	}
	// Relies on the fact that the records are ordered by offset.
	if len(records) > 0 && records[0].Offset != offset {
		trace_log.Error("Error in pulling records from record_log table. Some entries have been purged.", zap.String("collectionId", collectionId), zap.Int("requestedOffset", int(offset)), zap.Int("actualOffset", int(records[0].Offset)))
		records, err = nil, status.Error(codes.NotFound, "Some entries have been purged")
		return
	}
	// This means that the log is empty i.e. compaction_offset = enumeration_offset
	// AND also all records have been purged. In this case, if the requested offset
	// is less than the compacted offset (or enumeration offset), we should return an error.
	if len(records) == 0 {
		var compacted_offset, offset_err = r.GetLastCompactedOffsetForCollection(ctx, collectionId)
		// Can happen that no row exists in the collection table if no compaction
		// has ever happened for this collection or if the collection has been garbage
		// collected.
		if errors.Is(offset_err, pgx.ErrNoRows) {
			compacted_offset = 0
			offset_err = nil
		}
		if offset_err != nil {
			trace_log.Error("Error in getting last compacted offset", zap.Error(offset_err), zap.String("collectionId", collectionId))
			records, err = nil, status.Error(codes.NotFound, "Error in getting last compacted offset")
			return
		}
		if offset <= compacted_offset {
			trace_log.Error("Error in pulling records from record_log table. Some entries have been purged.", zap.String("collectionId", collectionId), zap.Int("requestedOffset", int(offset)), zap.Int("actualOffset", int(compacted_offset)))
			records, err = nil, status.Error(codes.NotFound, "Some entries have been purged")
			return
		}
	}
	return
}

func (r *LogRepository) ForkRecords(ctx context.Context, sourceCollectionID string, targetCollectionID string) (compactionOffset uint64, enumerationOffset uint64, err error) {
	var tx pgx.Tx
	tx, err = r.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		trace_log.Error("Error in begin transaction for forking logs in log service", zap.Error(err), zap.String("sourceCollectionID", sourceCollectionID))
		return
	}
	queriesWithTx := r.queries.WithTx(tx)
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	sourceBounds, err := queriesWithTx.GetBoundsForCollection(ctx, sourceCollectionID)
	if err != nil {
		trace_log.Error("Error in getting compaction and enumeration offset for source collection", zap.Error(err), zap.String("collectionId", sourceCollectionID))
		return
	}
	err = queriesWithTx.ForkCollectionRecord(ctx, log.ForkCollectionRecordParams{
		CollectionID:   sourceCollectionID,
		CollectionID_2: targetCollectionID,
	})
	if err != nil {
		trace_log.Error("Error forking log record", zap.String("sourceCollectionID", sourceCollectionID))
		return
	}
	targetBounds, err := queriesWithTx.GetMinimumMaximumOffsetForCollection(ctx, targetCollectionID)
	if err != nil {
		trace_log.Error("Error in deriving compaction and enumeration offset for target collection", zap.Error(err), zap.String("collectionId", targetCollectionID))
		return
	}

	if targetBounds.MinOffset == 0 {
		// Either the source collection is not compacted yet or no log is forked
		compactionOffset = uint64(sourceBounds.RecordCompactionOffsetPosition)
	} else {
		// Some logs are forked, the min offset is guaranteed to be larger than source compaction offset
		compactionOffset = uint64(targetBounds.MinOffset - 1)
	}
	if targetBounds.MaxOffset == 0 {
		// Either the source collection is empty or no log is forked
		enumerationOffset = uint64(sourceBounds.RecordEnumerationOffsetPosition)
	} else {
		// Some logs are forked. The max offset is the enumeration offset
		enumerationOffset = uint64(targetBounds.MaxOffset)
	}

	_, err = queriesWithTx.InsertCollection(ctx, log.InsertCollectionParams{
		ID:                              targetCollectionID,
		RecordCompactionOffsetPosition:  int64(compactionOffset),
		RecordEnumerationOffsetPosition: int64(enumerationOffset),
	})
	if err != nil {
		trace_log.Error("Error in updating offset for target collection", zap.Error(err), zap.String("collectionId", targetCollectionID))
		return
	}
	return
}

func (r *LogRepository) SealCollection(ctx context.Context, collectionID string) (err error) {
	_, err = r.queries.SealLog(ctx, collectionID)
	if err != nil && strings.Contains(err.Error(), "no rows in result set") {
		_, err = r.queries.SealLogInsert(ctx, collectionID)
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

func (r *LogRepository) GetLastCompactedOffsetForCollection(ctx context.Context, collectionId string) (compacted_offset int64, err error) {
	compacted_offset, err = r.queries.GetLastCompactedOffset(ctx, collectionId)
	if err != nil {
		trace_log.Error("Error in getting last compacted offset for collection", zap.Error(err), zap.String("collectionId", collectionId))
	}
	return
}

// GetBoundsForCollection returns the offset of the last record compacted and the offset of the last
// record inserted.  Thus, the range of uncompacted records is the interval (start, limit], which is
// kind of backwards from how it is elsewhere, so pay attention to comments indicating the bias of
// the offset.
func (r *LogRepository) GetBoundsForCollection(ctx context.Context, collectionId string) (start, limit int64, err error) {
	bounds, err := r.queries.GetBoundsForCollection(ctx, collectionId)
	if err != nil {
		trace_log.Error("Error in getting minimum and maximum offset for collection", zap.Error(err), zap.String("collectionId", collectionId))
		return
	}
	start = bounds.RecordCompactionOffsetPosition
	limit = bounds.RecordEnumerationOffsetPosition
	err = nil
	return
}

func (r *LogRepository) GarbageCollection(ctx context.Context) error {
	collectionToCompact, err := r.queries.GetAllCollections(ctx)
	if err != nil {
		trace_log.Error("Error in getting collections to compact", zap.Error(err))
		return err
	}
	trace_log.Info("Obtained collections to compact", zap.Int("collectionCount", len(collectionToCompact)))
	if collectionToCompact == nil {
		return nil
	}
	collectionsToGC := make([]string, 0)
	// TODO(Sanket): Make batch size configurable
	batchSize := 5000
	for i := 0; i < len(collectionToCompact); i += batchSize {
		end := min(len(collectionToCompact), i+batchSize)
		exists, err := r.sysDb.CheckCollections(ctx, collectionToCompact[i:end])
		trace_log.Info("Checking collections in sysdb", zap.Int("collectionCount", len(collectionToCompact[i:end])))
		if err != nil {
			trace_log.Error("Error in checking collection in sysdb", zap.Error(err))
			continue
		}
		for offset, exist := range exists {
			if !exist {
				collectionsToGC = append(collectionsToGC, collectionToCompact[offset+i])
			}
		}
	}
	trace_log.Info("Obtained collections to GC", zap.Int("collectionCount", len(collectionsToGC)))

	for _, collectionId := range collectionsToGC {
		var tx pgx.Tx
		tx, err = r.conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			trace_log.Error("Error in begin transaction for garbage collection", zap.Error(err))
			tx.Rollback(ctx)
			return err
		}
		queriesWithTx := r.queries.WithTx(tx)

		trace_log.Info("Deleting records for collection", zap.String("collectionId", collectionId))
		minMax, err := queriesWithTx.GetMinimumMaximumOffsetForCollection(ctx, collectionId)
		if err != nil {
			trace_log.Error("Error in getting minimum and maximum offset for collection", zap.Error(err), zap.String("collectionId", collectionId))
			tx.Rollback(ctx)
			continue
		}
		trace_log.Info("Obtained minimum and maximum offset for collection", zap.String("collectionId", collectionId), zap.Int64("minOffset", minMax.MinOffset), zap.Int64("maxOffset", minMax.MaxOffset))
		minOffset := minMax.MinOffset
		if minOffset == 1 {
			minOffset = 0
		}
		maxOffset := minMax.MaxOffset
		batchSize := max(1, min(int(maxOffset-minOffset), 100))
		for offset := minOffset; offset <= maxOffset; offset += int64(batchSize) {
			err = queriesWithTx.DeleteRecordsRange(ctx, log.DeleteRecordsRangeParams{
				CollectionID: collectionId,
				MinOffset:    minOffset,
				MaxOffset:    min(offset+int64(batchSize), maxOffset+1),
			})
			if err != nil {
				trace_log.Error("Error in deleting records for collection", zap.Error(err), zap.String("collectionId", collectionId))
				tx.Rollback(ctx)
				continue
			}
		}

		if err != nil {
			trace_log.Error("Error in deleting records for collection", zap.Error(err), zap.String("collectionId", collectionId))
			tx.Rollback(ctx)
			continue
		}

		trace_log.Info("Deleted records for collection", zap.String("collectionId", collectionId))

		err = queriesWithTx.DeleteCollection(ctx, collectionsToGC)
		if err != nil {
			trace_log.Error("Error in deleting collection", zap.Error(err))
			tx.Rollback(ctx)
			return err
		}

		tx.Commit(ctx)
	}

	trace_log.Info("Garbage collection completed", zap.Strings("collections", collectionsToGC))

	return nil
}

func NewLogRepository(conn *pgxpool.Pool, sysDb sysdb.ISysDB) *LogRepository {
	return &LogRepository{
		conn:    conn,
		queries: log.New(conn),
		sysDb:   sysDb,
	}
}
