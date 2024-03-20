package dao

import (
	"database/sql"
	"errors"
	"github.com/chroma-core/chroma/go/pkg/logservice/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
)

//go:generate mockery --name=IRecordLogDb
type IRecordLogDb interface {
	PushLogs(collectionID types.UniqueID, recordsContent [][]byte) (int, error)
	PullLogs(collectionID types.UniqueID, id int64, batchSize int) ([]*dbmodel.RecordLog, error)
	GetAllCollectionsToCompact() ([]*dbmodel.RecordLog, error)
}

type recordLogDb struct {
	db *gorm.DB
}

func (s *recordLogDb) PushLogs(collectionID types.UniqueID, recordsContent [][]byte) (int, error) {
	err := s.db.Transaction(func(tx *gorm.DB) error {
		var timestamp = time.Now().UnixNano()
		var collectionIDStr = types.FromUniqueID(collectionID)
		log.Info("PushLogs",
			zap.String("collectionID", *collectionIDStr),
			zap.Int64("timestamp", timestamp),
			zap.Int("count", len(recordsContent)))

		var lastLog *dbmodel.RecordLog
		err := tx.Select("id").Where("collection_id = ?", collectionIDStr).Last(&lastLog).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Error("Get last log id error", zap.Error(err))
			tx.Rollback()
			return err
		}
		var lastLogId = lastLog.ID
		log.Info("PushLogs", zap.Int64("lastLogId", lastLogId))

		var recordLogs []*dbmodel.RecordLog
		for index := range recordsContent {
			recordLogs = append(recordLogs, &dbmodel.RecordLog{
				CollectionID: collectionIDStr,
				ID:           lastLogId + int64(index) + 1,
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
		if lastLogId == 0 {
			// Check if collection position exists
			var collectionPosition dbmodel.CollectionPosition
			err = tx.Where("id = ?", collectionIDStr).Find(&collectionPosition).Error
			if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
				tx.Create(&dbmodel.CollectionPosition{
					ID:          *collectionIDStr,
					LogPosition: 0,
				})
			}
		}
		return nil
	})
	if err != nil {
		log.Error("PushLogs error", zap.Error(err))
		return 0, err
	}
	return len(recordsContent), nil
}

func (s *recordLogDb) PullLogs(collectionID types.UniqueID, id int64, batchSize int) ([]*dbmodel.RecordLog, error) {
	var collectionIDStr = types.FromUniqueID(collectionID)
	log.Info("PullLogs",
		zap.String("collectionID", *collectionIDStr),
		zap.Int64("ID", id),
		zap.Int("batch_size", batchSize))

	var recordLogs []*dbmodel.RecordLog
	result := s.db.Where("collection_id = ? AND id >= ?", collectionIDStr, id).Order("id").Limit(batchSize).Find(&recordLogs)
	if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		log.Error("PullLogs error", zap.Error(result.Error))
		return nil, result.Error
	}
	log.Info("PullLogs",
		zap.String("collectionID", *collectionIDStr),
		zap.Int64("ID", id),
		zap.Int("batch_size", batchSize),
		zap.Int("count", len(recordLogs)))
	return recordLogs, nil
}

func (s *recordLogDb) GetAllCollectionsToCompact() ([]*dbmodel.RecordLog, error) {
	log.Info("GetAllCollectionsToCompact")
	var recordLogs []*dbmodel.RecordLog
	var rawSql = `
	    with summary as (
		  select r.collection_id, r.id, r.timestamp, row_number() over(partition by r.collection_id order by r.id) as rank
		  from record_logs r, collection_position c
		  where r.collection_id = c.id
		  and r.id>c.log_position
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
