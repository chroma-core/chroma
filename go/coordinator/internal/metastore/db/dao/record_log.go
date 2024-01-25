package dao

import (
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type recordLogDb struct {
	db *gorm.DB
}

func (s *recordLogDb) PushLogs(collectionID types.UniqueID, recordContent []string) error {
	var tso int64
	s.db.Raw("select @@tidb_current_ts").Scan(&tso)
	var collectionIDStr = types.FromUniqueID(collectionID)
	log.Info("PushLogs",
		zap.String("collectionID", *collectionIDStr),
		zap.Int64("ID", tso),
		zap.Int("count", len(recordContent)))

	var recordLogs []*dbmodel.RecordLog
	for index := range recordContent {
		recordLogs = append(recordLogs, &dbmodel.RecordLog{
			CollectionID: collectionIDStr,
			ID:           tso,
			Offset:       index,
			Record:       &recordContent[index],
		})
	}
	return s.db.CreateInBatches(recordLogs, len(recordLogs)).Error
}

func (s *recordLogDb) PullLogsFromID(collectionID types.UniqueID, id int64, batch_size int) ([]*dbmodel.RecordLog, error) {
	var collectionIDStr = types.FromUniqueID(collectionID)
	log.Info("PullLogsFromID",
		zap.String("collectionID", *collectionIDStr),
		zap.Int64("ID", id),
		zap.Int("batch_size", batch_size))

	var recordLogs []*dbmodel.RecordLog
	s.db.Where("collection_id = ? AND id >= ?", collectionIDStr, id).Order("id").Order("offset").Limit(batch_size).Find(&recordLogs)
	log.Info("PullLogsFromID",
		zap.String("collectionID", *collectionIDStr),
		zap.Int64("ID", id),
		zap.Int("batch_size", batch_size),
		zap.Int("count", len(recordLogs)))
	return recordLogs, nil
}
