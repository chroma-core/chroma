package dbmodel

import (
	"github.com/chroma-core/chroma/go/internal/types"
)

type RecordLog struct {
	CollectionID *string `gorm:"collection_id;primaryKey;autoIncrement:false"`
	ID           int64   `gorm:"id;primaryKey;autoIncrement:false"`
	Timestamp    int64   `gorm:"timestamp;"`
	Record       *[]byte `gorm:"record;type:bytea"`
}

func (v RecordLog) TableName() string {
	return "record_logs"
}

//go:generate mockery --name=IRecordLogDb
type IRecordLogDb interface {
	PushLogs(collectionID types.UniqueID, recordsContent [][]byte) (int, error)
	PullLogs(collectionID types.UniqueID, id int64, batchSize int) ([]*RecordLog, error)
	GetAllCollectionsToCompact() ([]*RecordLog, error)
}
