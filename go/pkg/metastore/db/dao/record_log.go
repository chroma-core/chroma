package dao

import (
	"database/sql"
	"errors"
	"time"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type recordLogDb struct {
	db *gorm.DB
}

var _ dbmodel.IRecordLogDb = &recordLogDb{}

func (s *recordLogDb) PushLogs(collectionID types.UniqueID, recordsContent [][]byte) (int, error) {
	err := s.db.Transaction(func(tx *gorm.DB) error {
		var timestamp = time.Now().UnixNano()
		var collectionIDStr = types.FromUniqueID(collectionID)
		log.Info("PushLogs",
			zap.String("collectionID", *collectionIDStr),
			zap.Int64("timestamp", timestamp),
			zap.Int("count", len(recordsContent)))

		var lastLog *dbmodel.RecordLog
		err := tx.Select("log_offset").Where("collection_id = ?", collectionIDStr).Order("log_offset DESC").Limit(1).Find(&lastLog).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Error("Get last log offset error", zap.Error(err))
			tx.Rollback()
			return err
		}
		// The select will populate the lastLog with the last log in the collection, if
		// one does not exist, it will have a default value of 0, so we can safely use it
		var lastLogOffset = lastLog.LogOffset
		log.Info("PushLogs", zap.Int64("lastLogOffset", lastLogOffset))

		var recordLogs []*dbmodel.RecordLog
		for index := range recordsContent {
			recordLogs = append(recordLogs, &dbmodel.RecordLog{
				CollectionID: collectionIDStr,
				LogOffset:    lastLogOffset + int64(index) + 1,
				Timestamp:    timestamp,
				Record:       &recordsContent[index],
			})
		}
		err = tx.CreateInBatches(recordLogs, len(recordLogs)).Error
		if err != nil {
			log.Error("Batch insert error", zap.Error(err))
			tx.Rollback()
			return err
		}
		return nil
	})
	if err != nil {
		log.Error("PushLogs error", zap.Error(err))
		return 0, err
	}
	return len(recordsContent), nil
}

func (s *recordLogDb) PullLogs(collectionID types.UniqueID, offset int64, batchSize int, endTimestamp int64) ([]*dbmodel.RecordLog, error) {
	var collectionIDStr = types.FromUniqueID(collectionID)
	log.Info("PullLogs",
		zap.String("collectionID", *collectionIDStr),
		zap.Int64("log_offset", offset),
		zap.Int("batch_size", batchSize),
		zap.Int64("endTimestamp", endTimestamp))

	var recordLogs []*dbmodel.RecordLog
	if endTimestamp > 0 {
		result := s.db.Where("collection_id = ? AND log_offset >= ? AND timestamp <= ?", collectionIDStr, offset, endTimestamp).Order("log_offset").Limit(batchSize).Find(&recordLogs)
		if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			log.Error("PullLogs error", zap.Error(result.Error))
			return nil, result.Error
		}
	} else {
		result := s.db.Where("collection_id = ? AND log_offset >= ?", collectionIDStr, offset).Order("log_offset").Limit(batchSize).Find(&recordLogs)
		if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			log.Error("PullLogs error", zap.Error(result.Error))
			return nil, result.Error
		}
	}
	log.Info("PullLogs",
		zap.String("collectionID", *collectionIDStr),
		zap.Int64("log_offset", offset),
		zap.Int("batch_size", batchSize),
		zap.Int("count", len(recordLogs)))
	return recordLogs, nil
}

func (s *recordLogDb) GetAllCollectionsToCompact() ([]*dbmodel.RecordLog, error) {
	log.Info("GetAllCollectionsToCompact")
	var recordLogs []*dbmodel.RecordLog
	var rawSql = `
	    with summary as (
		  select r.collection_id, r.log_offset, r.timestamp, row_number() over(partition by r.collection_id order by r.log_offset) as rank
		  from record_logs r, collections c
		  where r.collection_id = c.id
		  and r.log_offset>c.log_position
		)
		select * from summary
		where rank=1
		order by timestamp;`
	rows, err := s.db.Raw(rawSql).Rows()
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Error("GetAllCollectionsToCompact Close error", zap.Error(err))
		}
	}(rows)
	if err != nil {
		log.Error("GetAllCollectionsToCompact error", zap.Error(err))
		return nil, err
	}
	for rows.Next() {
		var batchRecordLogs []*dbmodel.RecordLog
		err := s.db.ScanRows(rows, &recordLogs)
		if err != nil {
			log.Error("GetAllCollectionsToCompact ScanRows error", zap.Error(err))
			return nil, err
		}
		recordLogs = append(recordLogs, batchRecordLogs...)
	}
	log.Info("GetAllCollectionsToCompact find collections count", zap.Int("count", len(recordLogs)))
	return recordLogs, nil
}
